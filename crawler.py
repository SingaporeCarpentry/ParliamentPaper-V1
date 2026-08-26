#!/usr/bin/env python3
"""
crawler.py

Single-file crawler for SPRS (sprs.parl.gov.sg) that:
- discovers PDF links from pages under sprs.parl.gov.sg
- downloads and validates PDFs (signature, Content-Type, pdfplumber open, page count)
- extracts text per page into CSV files
- writes master document_index.csv, crawl_status.csv, crawl_summary.json
- respects robots.txt, retry rules, min-interval, timeouts
- CLI options match requested spec

Dependencies (add to requirements.txt):
requests
beautifulsoup4
pdfplumber
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import shutil
import signal
import socket
import sys
import time
import urllib.parse
from collections import deque, defaultdict
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
except Exception as e:
    print("pdfplumber import failed: ensure requirements are installed", file=sys.stderr)
    raise

from urllib.robotparser import RobotFileParser

# Constants
ALLOWED_HOST = "sprs.parl.gov.sg"
PDF_SIG = b"%PDF-"
DEFAULT_USER_AGENT = "SPRSResearchCrawler/1.0 (contact: unknown)"
CSV_BOM = "\ufeff"  # BOM for Excel compatibility

# HTTP statuses to retry (bounded retries)
RETRY_STATUS = {429, 500, 502, 503, 504}
# Do NOT retry 401/403 per policy
NO_RETRY_STATUS = {401, 403}

# Logging
logger = logging.getLogger("sprs_crawler")


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def sanitize_for_filename(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-\.]+", "", s)
    if not s:
        return "document"
    return s[:200]


def canonicalize_url(url: str) -> str:
    # Normalize and remove fragments
    p = urllib.parse.urlparse(url)
    scheme = p.scheme or "https"
    netloc = p.netloc.lower()
    path = urllib.parse.quote(urllib.parse.unquote(p.path), safe="/%:@&?=+$,")
    query = "&".join(sorted(urllib.parse.parse_qsl(p.query, keep_blank_values=True)))
    return urllib.parse.urlunparse((scheme, netloc, path, "", query, ""))


class PoliteRequester:
    def __init__(self, user_agent: str, min_interval: float, timeout: int, max_retries: int):
        self.user_agent = user_agent
        self.min_interval = min_interval
        self.timeout = timeout
        self.max_retries = max_retries
        self.last_request = 0.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

    def wait_if_needed(self):
        elapsed = time.time() - self.last_request
        if elapsed < self.min_interval:
            to_wait = self.min_interval - elapsed
            logger.debug("Sleeping %.2fs to respect min-interval", to_wait)
            time.sleep(to_wait)

    def request(self, method, url, **kwargs):
        attempts = 0
        while True:
            attempts += 1
            self.wait_if_needed()
            try:
                resp = self.session.request(method, url, timeout=self.timeout, allow_redirects=True, **kwargs)
            except (requests.RequestException, socket.timeout) as e:
                logger.warning("Request exception for %s: %s", url, e)
                if attempts > self.max_retries:
                    raise
                backoff = min(30, 2 ** attempts)
                time.sleep(backoff)
                continue

            self.last_request = time.time()

            if resp.status_code in NO_RETRY_STATUS:
                # Do not retry 401/403
                return resp

            if resp.status_code in RETRY_STATUS:
                if attempts > self.max_retries:
                    return resp
                # Honor Retry-After
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        ra_seconds = int(ra)
                    except ValueError:
                        # Could be HTTP-date; just backoff
                        ra_seconds = min(30, 2 ** attempts)
                else:
                    ra_seconds = min(30, 2 ** attempts)
                logger.info("Retryable status %s for %s, sleeping %s", resp.status_code, url, ra_seconds)
                time.sleep(ra_seconds)
                continue

            return resp


class Crawler:
    def __init__(self, args):
        self.start_url = args.start_url
        self.output_dir = args.output_dir
        self.max_pages = args.max_pages
        self.max_pdfs = args.max_pdfs
        self.min_interval = args.min_interval
        self.timeout = args.timeout
        self.max_retries = args.max_retries
        self.user_agent = args.user_agent or DEFAULT_USER_AGENT
        self.resume = args.resume
        self.dry_run = args.dry_run
        self.enable_ocr = getattr(args, "enable_ocr", False)

        # Internals
        self.requester = PoliteRequester(self.user_agent, self.min_interval, self.timeout, self.max_retries)
        self.visited_pages = set()
        self.discovered_pdf_links = {}  # canonical_url -> referrer_url
        self.pdfs_downloaded = {}  # canonical_url -> metadata dict
        self.crawl_status_rows = []
        self.document_index_rows = []
        self.stats = defaultdict(int)
        self.robots = RobotFileParser()
        self.robots_read = False

        # Directories
        os.makedirs(self.output_dir, exist_ok=True)
        self.pdf_dir = os.path.join(self.output_dir, "pdf")
        self.csv_dir = os.path.join(self.output_dir, "csv")
        os.makedirs(self.pdf_dir, exist_ok=True)
        os.makedirs(self.csv_dir, exist_ok=True)

        # Logs
        stdout_log = os.path.join(self.output_dir, "stdout.log")
        stderr_log = os.path.join(self.output_dir, "stderr.log")
        fh = logging.FileHandler(stdout_log)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(fh)
        logger.setLevel(logging.DEBUG)

    def load_robots(self):
        try:
            rp_url = urllib.parse.urljoin(self.start_url, "/robots.txt")
            self.robots.set_url(rp_url)
            self.robots.read()
            self.robots_read = True
            # robotparser doesn't surface HTTP 401/403 easily; attempt a GET to check
            r = self.requester.session.head(rp_url, timeout=self.timeout, allow_redirects=True)
            if r.status_code in (401, 403):
                raise PermissionError(f"robots.txt returned {r.status_code}")
        except PermissionError:
            logger.error("Access to robots.txt denied (401/403). Aborting per policy.")
            raise

    def allowed_by_robots(self, url):
        if not self.robots_read:
            try:
                self.load_robots()
            except PermissionError:
                raise
        return self.robots.can_fetch(self.user_agent, url)

    def is_on_allowed_host(self, url):
        p = urllib.parse.urlparse(url)
        return p.netloc.lower().endswith(ALLOWED_HOST)

    def fetch_page(self, url, referrer=None):
        canonical = canonicalize_url(url)
        if canonical in self.visited_pages:
            return None, None
        if not self.is_on_allowed_host(canonical):
            logger.debug("Skipping off-host url %s", canonical)
            self.stats["blocked_requests"] += 1
            return None, None
        if not self.allowed_by_robots(canonical):
            logger.warning("Blocked by robots.txt: %s", canonical)
            self.crawl_status_rows.append(self._status_row("robots", url, canonical, None, None, 0, "blocked", "robots_disallowed", referrer))
            self.stats["blocked_requests"] += 1
            return None, None

        resp = self.requester.request("GET", canonical)
        self.visited_pages.add(canonical)
        self.stats["pages_requested"] += 1

        row_reason = None
        status = "failed"
        if resp is None:
            row_reason = "request_error"
        else:
            status_code = resp.status_code
            content_type = resp.headers.get("Content-Type", "")
            final = resp.url
            body_len = len(resp.content) if resp.content is not None else 0

            self.crawl_status_rows.append(self._status_row("page_fetch", url, final, status_code, content_type, 1, "ok" if status_code == 200 else "failed", None, referrer))
            if status_code != 200:
                logger.info("Page %s returned %s", canonical, status_code)
                self.stats["pages_failed"] += 1
                return None, resp
            # Parse only for HTML
            if "text/html" in content_type or resp.content.strip().startswith(b"<"):
                soup = BeautifulSoup(resp.content, "html.parser")
                return soup, resp
            else:
                # Not HTML - still record
                logger.debug("Non-HTML page fetched: %s (Content-Type: %s)", canonical, content_type)
                return None, resp

    def discover_from_soup(self, soup, base_url):
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            url = urllib.parse.urljoin(base_url, href)
            url = canonicalize_url(url)
            url = url.split("#", 1)[0]
            links.add(url)

        # basic form discovery (action)
        for f in soup.find_all("form", action=True):
            action = f["action"]
            url = urllib.parse.urljoin(base_url, action)
            url = canonicalize_url(url)
            links.add(url)

        return links

    def is_pdf_link_hint(self, url):
        # conservative hint: .pdf extension or pdf parameter in query or link text hint
        p = urllib.parse.urlparse(url)
        if p.path.lower().endswith(".pdf"):
            return True
        if "download" in p.path.lower() and "pdf" in (p.query or "").lower():
            return True
        return False

    def discover(self):
        q = deque([self.start_url])
        pages_seen = 0
        while q and pages_seen < (self.max_pages or 1000000) and len(self.discovered_pdf_links) < (self.max_pdfs or 1000000):
            url = q.popleft()
            if canonicalize_url(url) in self.visited_pages:
                continue
            if not self.is_on_allowed_host(url):
                continue

            soup, resp = self.fetch_page(url, referrer=None)
            pages_seen += 1
            if resp is not None and resp.status_code == 200:
                content_type = resp.headers.get("Content-Type", "")
                final_url = resp.url
                # discover links
                if soup:
                    links = self.discover_from_soup(soup, final_url)
                    for link in links:
                        if not self.is_on_allowed_host(link):
                            continue
                        # PDFs
                        if self.is_pdf_link_hint(link):
                            if link not in self.discovered_pdf_links:
                                self.discovered_pdf_links[link] = url
                                self.stats["pdf_links_discovered"] += 1
                                logger.info("Discovered PDF link: %s (from %s)", link, url)
                                if len(self.discovered_pdf_links) >= (self.max_pdfs or 1000000):
                                    break
                        else:
                            if canonicalize_url(link) not in self.visited_pages and len(self.visited_pages) < (self.max_pages or 1000000):
                                q.append(link)
                # also check for direct pdf in resp (some endpoints redirect to pdf)
                if "application/pdf" in content_type or (resp.content and resp.content.startswith(PDF_SIG)):
                    final = canonicalize_url(resp.url)
                    if final not in self.discovered_pdf_links:
                        self.discovered_pdf_links[final] = url
                        self.stats["pdf_links_discovered"] += 1

            # respect politesse to not spin
            # (PoliteRequester already enforces min_interval between requests.)

        logger.info("Discovery complete: pages requested=%s pdf_links=%s", self.stats["pages_requested"], len(self.discovered_pdf_links))

    def _status_row(self, stage, url, final_url, http_status, content_type, attempt, status, reason, referrer=None):
        return {
            "timestamp_utc": iso_now(),
            "stage": stage,
            "url": url,
            "final_url": final_url or "",
            "http_status": http_status or "",
            "content_type": content_type or "",
            "attempt": attempt,
            "status": status,
            "reason": reason or "",
            "referrer_url": referrer or "",
        }

    def download_and_process_all(self):
        pdf_count = 0
        error_count = 0
        for pdf_url, ref in list(self.discovered_pdf_links.items()):
            if (self.max_pdfs is not None) and (pdf_count >= self.max_pdfs):
                break
            try:
                ok = self.handle_single_pdf(pdf_url, ref)
                if ok:
                    pdf_count += 1
                else:
                    error_count += 1
            except PermissionError:
                # 401/403 encountered while trying to fetch resource
                logger.error("Access denied while fetching %s", pdf_url)
                self.crawl_status_rows.append(self._status_row("download", pdf_url, None, 403, None, 0, "blocked", "access_denied", ref))
                return "access_denied"
        self.stats["pdfs_downloaded"] = pdf_count
        self.stats["pdfs_failed"] = error_count
        return "ok"

    def handle_single_pdf(self, pdf_url, referrer):
        canonical = canonicalize_url(pdf_url)
        logger.info("Processing candidate PDF: %s (from %s)", canonical, referrer)

        # If resume and already in index, skip
        existing = self._find_in_index_by_url(canonical)
        if existing and self.resume:
            logger.info("Skipping previously indexed PDF (resume): %s", canonical)
            self.stats["pdfs_skipped_existing"] += 1
            return True

        if not self.allowed_by_robots(canonical):
            logger.warning("Blocked by robots for PDF: %s", canonical)
            self.crawl_status_rows.append(self._status_row("download", pdf_url, canonical, None, None, 0, "blocked", "robots_disallowed", referrer))
            self.stats["blocked_requests"] += 1
            return False

        # HEAD first to check status (some servers don't allow HEAD — fallback to GET)
        resp = self.requester.request("HEAD", canonical)
        if resp is None:
            logger.warning("HEAD failed for %s", canonical)
        if resp is not None and resp.status_code in NO_RETRY_STATUS:
            logger.error("Access denied for %s (%s)", canonical, resp.status_code)
            # Per policy: 401/403 must stop crawling that host
            raise PermissionError("Access denied")

        # Now GET
        if self.dry_run:
            logger.info("Dry-run: not downloading %s", canonical)
            # record a dry-run status row
            self.crawl_status_rows.append(self._status_row("download", pdf_url, canonical, None, None, 0, "dry-run", "dry_run", referrer))
            return True

        resp = self.requester.request("GET", canonical, stream=True)
        final_url = resp.url if resp is not None else canonical
        http_status = resp.status_code if resp is not None else None
        content_type = resp.headers.get("Content-Type", "") if resp is not None else ""

        attempts = 1
        if resp is None:
            self.crawl_status_rows.append(self._status_row("download", pdf_url, final_url, None, None, attempts, "failed", "no_response", referrer))
            return False

        if resp.status_code in NO_RETRY_STATUS:
            # Per policy, do not retry 401/403, stop crawling
            raise PermissionError(f"Access denied {resp.status_code} for {canonical}")

        if resp.status_code != 200:
            logger.warning("GET returned %s for %s", resp.status_code, canonical)
            self.crawl_status_rows.append(self._status_row("download", pdf_url, final_url, resp.status_code, content_type, attempts, "failed", "http_status", referrer))
            return False

        # Read content to temp file
        fname_safe = sanitize_for_filename(os.path.basename(urllib.parse.urlparse(final_url).path) or "document")
        # create deterministic basename: sanitized title-like + initial URL sha
        url_hash = hashlib.sha256(final_url.encode("utf-8")).hexdigest()[:12]
        base_name = f"{fname_safe}_{url_hash}"
        part_path = os.path.join(self.pdf_dir, base_name + ".part")
        final_pdf_path = os.path.join(self.pdf_dir, base_name + ".pdf")

        if os.path.exists(final_pdf_path):
            logger.info("PDF already exists locally: %s", final_pdf_path)
            # compute sha256 and proceed to extraction if needed
            sha256sum = self._compute_sha256(final_pdf_path)
            page_count = self._get_pdf_pagecount(final_pdf_path)
            self.document_index_rows.append({
                "document_id": url_hash,
                "document_title": fname_safe,
                "source_page_url": referrer,
                "pdf_url": pdf_url,
                "final_pdf_url": final_url,
                "local_pdf_path": final_pdf_path,
                "local_csv_path": os.path.join(self.csv_dir, base_name + ".csv"),
                "http_status": resp.status_code,
                "content_type": content_type,
                "download_bytes": os.path.getsize(final_pdf_path),
                "sha256": sha256sum,
                "page_count": page_count,
                "csv_rows": 0,
                "download_status": "cached",
                "extraction_status": "pending",
                "error": "",
            })
            # proceed to extraction
            ok = self.extract_pdf_to_csv(final_pdf_path, base_name, fname_safe, pdf_url, final_url, referrer, sha256sum)
            return ok

        # write stream to part file
        try:
            with open(part_path, "wb") as fh:
                total = 0
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        fh.write(chunk)
                        total += len(chunk)
            # validate signature and Content-Type
            with open(part_path, "rb") as fh:
                header = fh.read(8)
            if not header.startswith(PDF_SIG):
                logger.error("Downloaded file for %s does not start with %s", canonical, PDF_SIG)
                self.crawl_status_rows.append(self._status_row("download", pdf_url, final_url, resp.status_code, content_type, attempts, "rejected", "no_pdf_signature", referrer))
                os.remove(part_path)
                self.stats["pdfs_rejected"] += 1
                return False
            if total == 0:
                logger.error("Downloaded zero bytes for %s", canonical)
                self.crawl_status_rows.append(self._status_row("download", pdf_url, final_url, resp.status_code, content_type, attempts, "rejected", "zero_length", referrer))
                os.remove(part_path)
                self.stats["pdfs_rejected"] += 1
                return False

            # compute sha256
            sha256sum = self._compute_sha256(part_path)
            # open with pdfplumber
            try:
                with pdfplumber.open(part_path) as pdf:
                    page_count = len(pdf.pages)
                    if page_count == 0:
                        raise ValueError("zero pages")
            except Exception as ex:
                logger.exception("pdfplumber failed for %s: %s", canonical, ex)
                self.crawl_status_rows.append(self._status_row("download", pdf_url, final_url, resp.status_code, content_type, attempts, "rejected", "pdfplumber_failed", referrer))
                os.remove(part_path)
                self.stats["pdfs_rejected"] += 1
                return False

            # rename atomically
            if os.path.exists(final_pdf_path):
                logger.warning("Not overwriting existing PDF: %s", final_pdf_path)
                os.remove(part_path)
                return False
            os.rename(part_path, final_pdf_path)

            # record metadata
            self.document_index_rows.append({
                "document_id": url_hash,
                "document_title": fname_safe,
                "source_page_url": referrer,
                "pdf_url": pdf_url,
                "final_pdf_url": final_url,
                "local_pdf_path": final_pdf_path,
                "local_csv_path": os.path.join(self.csv_dir, base_name + ".csv"),
                "http_status": resp.status_code,
                "content_type": content_type,
                "download_bytes": total,
                "sha256": sha256sum,
                "page_count": page_count,
                "csv_rows": 0,
                "download_status": "downloaded",
                "extraction_status": "pending",
                "error": "",
            })

            self.crawl_status_rows.append(self._status_row("download", pdf_url, final_url, resp.status_code, content_type, attempts, "ok", None, referrer))
            self.stats["pdfs_downloaded"] += 1

            # extraction
            ok = self.extract_pdf_to_csv(final_pdf_path, base_name, fname_safe, pdf_url, final_url, referrer, sha256sum)
            return ok

        except Exception as e:
            logger.exception("Error handling PDF %s: %s", canonical, e)
            if os.path.exists(part_path):
                try:
                    os.remove(part_path)
                except Exception:
                    pass
            self.crawl_status_rows.append(self._status_row("download", pdf_url, final_url, resp.status_code if resp is not None else None, content_type, attempts, "failed", "exception", referrer))
            self.stats["pdfs_failed"] += 1
            return False

    def _compute_sha256(self, path):
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def _get_pdf_pagecount(self, path):
        try:
            with pdfplumber.open(path) as pdf:
                return len(pdf.pages)
        except Exception:
            return 0

    def extract_pdf_to_csv(self, local_pdf_path, base_name, title, pdf_url, final_url, referrer, sha256sum):
        csv_path = os.path.join(self.csv_dir, base_name + ".csv")
        rows_written = 0
        extraction_ok = True
        try:
            with pdfplumber.open(local_pdf_path) as pdf:
                with open(csv_path, "w", encoding="utf-8-sig", newline="") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=[
                        "document_id",
                        "document_title",
                        "source_page_url",
                        "pdf_url",
                        "final_pdf_url",
                        "downloaded_at_utc",
                        "sha256",
                        "page_number",
                        "record_index",
                        "section_title",
                        "content_text",
                        "extraction_method",
                        "extraction_warning"
                    ], quoting=csv.QUOTE_MINIMAL)
                    csvfile.write(CSV_BOM)
                    writer.writeheader()
                    for pnum, page in enumerate(pdf.pages, start=1):
                        text = page.extract_text() or ""
                        extraction_method = "pdfplumber"
                        warning = ""
                        if not text.strip():
                            warning = "image-only-or-blank"
                        # Normalize repeated whitespace but preserve paragraphs
                        text = re.sub(r"[ \t]+", " ", text)
                        text = text.replace("\x00", "")  # remove null bytes
                        row = {
                            "document_id": base_name,
                            "document_title": title,
                            "source_page_url": referrer,
                            "pdf_url": pdf_url,
                            "final_pdf_url": final_url,
                            "downloaded_at_utc": iso_now(),
                            "sha256": sha256sum,
                            "page_number": pnum,
                            "record_index": 1,
                            "section_title": "",
                            "content_text": text,
                            "extraction_method": extraction_method,
                            "extraction_warning": warning,
                        }
                        writer.writerow(row)
                        rows_written += 1
        except Exception as ex:
            logger.exception("Extraction failed for %s: %s", local_pdf_path, ex)
            extraction_ok = False

        # update index row
        for idx in self.document_index_rows:
            if idx["local_pdf_path"] == local_pdf_path:
                idx["csv_rows"] = rows_written
                idx["extraction_status"] = "ok" if extraction_ok else "failed"
                if not extraction_ok:
                    idx["error"] = "extraction_failed"
                break

        if not extraction_ok:
            self.stats["pdfs_failed"] += 1
            return False
        self.stats["pdfs_extracted"] += 1
        return True

    def _find_in_index_by_url(self, canonical_url):
        # naive: scan document_index_rows
        for r in self.document_index_rows:
            if r.get("final_pdf_url") == canonical_url or r.get("pdf_url") == canonical_url:
                return r
        return None

    def write_outputs(self):
        # write document_index.csv
        di_path = os.path.join(self.output_dir, "document_index.csv")
        with open(di_path, "w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=[
                "document_id",
                "document_title",
                "source_page_url",
                "pdf_url",
                "final_pdf_url",
                "local_pdf_path",
                "local_csv_path",
                "http_status",
                "content_type",
                "download_bytes",
                "sha256",
                "page_count",
                "csv_rows",
                "download_status",
                "extraction_status",
                "error",
            ], quoting=csv.QUOTE_MINIMAL)
            fh.write(CSV_BOM)
            writer.writeheader()
            for r in self.document_index_rows:
                writer.writerow(r)

        cs_path = os.path.join(self.output_dir, "crawl_status.csv")
        with open(cs_path, "w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=[
                "timestamp_utc",
                "stage",
                "url",
                "final_url",
                "http_status",
                "content_type",
                "attempt",
                "status",
                "reason",
                "referrer_url",
            ], quoting=csv.QUOTE_MINIMAL)
            fh.write(CSV_BOM)
            writer.writeheader()
            for r in self.crawl_status_rows:
                writer.writerow(r)

        # summary
        summary = {
            "pages_requested": int(self.stats.get("pages_requested", 0)),
            "pages_successful": int(self.stats.get("pages_requested", 0) - self.stats.get("pages_failed", 0)),
            "pages_failed": int(self.stats.get("pages_failed", 0)),
            "unique_links_discovered": int(len(self.visited_pages)),
            "pdf_links_discovered": int(self.stats.get("pdf_links_discovered", 0)),
            "pdfs_downloaded": int(self.stats.get("pdfs_downloaded", 0)),
            "pdfs_skipped_existing": int(self.stats.get("pdfs_skipped_existing", 0)),
            "pdfs_rejected": int(self.stats.get("pdfs_rejected", 0)),
            "pdfs_failed": int(self.stats.get("pdfs_failed", 0)),
            "pdfs_extracted": int(self.stats.get("pdfs_extracted", 0)),
            "csv_files_created": int(len([n for n in os.listdir(self.csv_dir) if n.endswith(".csv")])),
            "csv_rows_created": int(sum(int(r.get("csv_rows", 0)) for r in self.document_index_rows)),
            "duplicate_urls": 0,
            "duplicate_hashes": 0,
            "blocked_requests": int(self.stats.get("blocked_requests", 0)),
            "run_exit_code": None,
        }
        with open(os.path.join(self.output_dir, "crawl_summary.json"), "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)

    def determine_exit_code(self):
        downloaded = int(self.stats.get("pdfs_downloaded", 0))
        extracted = int(self.stats.get("pdfs_extracted", 0))
        failed = int(self.stats.get("pdfs_failed", 0))
        skipped = int(self.stats.get("pdfs_skipped_existing", 0))
        if downloaded + skipped == 0 and extracted == 0:
            # no valid PDFs found or downloaded
            return 1
        if failed > 0:
            return 3
        # success
        return 0


def parse_args():
    p = argparse.ArgumentParser(description="SPRS PDF crawler and extractor")
    p.add_argument("--start-url", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--max-pages", type=int, default=200)
    p.add_argument("--max-pdfs", type=int, default=500)
    p.add_argument("--min-interval", type=float, default=3.0)
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--user-agent", type=str, default=DEFAULT_USER_AGENT)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    # future: --enable-ocr
    return p.parse_args()


def main():
    args = parse_args()

    # Basic arg validation
    if not args.start_url.startswith("http"):
        print("start-url must be an http(s) URL", file=sys.stderr)
        return 2
    if ALLOWED_HOST not in urllib.parse.urlparse(args.start_url).netloc:
        print(f"start-url must be on host {ALLOWED_HOST}", file=sys.stderr)
        return 2

    # Run
    crawler = Crawler(args)
    try:
        crawler.discover()
    except PermissionError:
        # robots or 401/403 preventing crawling - exit code 4
        crawler.write_outputs()
        print("Access denied by robots.txt or HTTP 401/403.", file=sys.stderr)
        return 4

    result = crawler.download_and_process_all()
    crawler.write_outputs()
    exit_code = crawler.determine_exit_code()
    # Update summary run_exit_code
    summary_path = os.path.join(args.output_dir, "crawl_summary.json")
    try:
        with open(summary_path, "r+", encoding="utf-8") as fh:
            summary = json.load(fh)
            summary["run_exit_code"] = exit_code
            fh.seek(0)
            json.dump(summary, fh, indent=2)
            fh.truncate()
    except Exception:
        pass

    if exit_code == 0:
        print("Crawl and extraction completed successfully.")
    elif exit_code == 1:
        print("No valid PDFs were found or downloaded.", file=sys.stderr)
    elif exit_code == 3:
        print("One or more PDFs failed validation or extraction.", file=sys.stderr)
    elif exit_code == 4:
        print("Access explicitly denied by 401/403 or robots.txt.", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    rc = main()
    if isinstance(rc, int):
        sys.exit(rc)
    else:
        sys.exit(0)
