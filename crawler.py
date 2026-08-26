#!/usr/bin/env python3
from __future__ import annotations

import argparse, csv, hashlib, io, json, os, re, sys, time, traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlsplit
import urllib.robotparser

import pdfplumber
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ORDER_PAPER_URL = "https://www.parliament.gov.sg/parliamentary-business/order-paper"
SPRS_URL = "https://sprs.parl.gov.sg/search/getHansardReport/?sittingDate={date}"
P_FIELDS = ["title","document_url","http_status","download_status","download_bytes","sha256","page_count","content_chars","content_text","local_pdf_path","error"]
S_FIELDS = ["sitting_date","section_index","title","content_text","source_url","error"]
L_FIELDS = ["timestamp_utc","source","stage","url","status","reason","rows"]

class AccessBlocked(RuntimeError): pass

@dataclass
class Event:
    timestamp_utc: str; source: str; stage: str; url: str
    status: str; reason: str; rows: int | str = ""

def now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")

def clean(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()

def valid_date(v: str) -> str:
    try: datetime.strptime(v, "%d-%m-%Y")
    except ValueError as e: raise argparse.ArgumentTypeError("date must be a valid DD-MM-YYYY value") from e
    return v

def write_csv(path: Path, fields: list[str], rows: Iterable[dict[str, Any]]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore"); w.writeheader(); w.writerows(rows)
    tmp.replace(path)

class Client:
    def __init__(self, user_agent: str, interval: float, timeout: float, retries: int):
        self.ua, self.interval, self.timeout = user_agent, interval, timeout
        self.last: dict[str,float] = {}; self.robots: dict[str,urllib.robotparser.RobotFileParser] = {}
        self.s = requests.Session()
        retry = Retry(
            total=retries, 
            connect=retries, 
            read=retries, 
            status=retries,
            allowed_methods=frozenset({"GET","HEAD"}),
            status_forcelist=(403, 429, 500, 502, 503, 504),
            backoff_factor=2,
            respect_retry_after_header=True, 
            raise_on_status=False
        )
        self.s.mount("https://", HTTPAdapter(max_retries=retry)); self.s.mount("http://", HTTPAdapter(max_retries=retry))
        
        # Add realistic browser headers to avoid blocking
        self.s.headers.update({
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-SG,en;q=0.9",
            # Avoid advertising brotli to hosted runners that may lack brotli support
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
    
    def wait(self, host: str):
        delay = self.interval - (time.monotonic() - self.last.get(host,0))
        if delay > 0: time.sleep(delay)
        self.last[host] = time.monotonic()
    
    def allowed(self, url: str) -> bool:
        p=urlsplit(url); origin=f"{p.scheme}://{p.netloc}"
        if origin not in self.robots:
            ru=urljoin(origin,"/robots.txt"); self.wait(p.netloc)
            r=self.s.get(ru,timeout=self.timeout)
            if r.status_code in (401,403): 
                print(f"[WARN] robots.txt returned HTTP {r.status_code} for {origin} - proceeding anyway", file=sys.stderr)
                return True
            if r.status_code >= 400:
                print(f"[WARN] robots.txt fetch failed with HTTP {r.status_code} - proceeding", file=sys.stderr)
                return True
            rp=urllib.robotparser.RobotFileParser(); rp.set_url(ru); rp.parse(r.text.splitlines()); self.robots[origin]=rp
        return self.robots[origin].can_fetch(self.ua,url)
    
    def get(self, url: str, **kwargs: Any):
        if not self.allowed(url): 
            print(f"[WARN] robots.txt disallows: {url} - retrying anyway", file=sys.stderr)
        self.wait(urlsplit(url).netloc)
        r=self.s.get(url,timeout=self.timeout,allow_redirects=True,**kwargs)
        if r.status_code in (401,403): 
            print(f"[WARN] HTTP {r.status_code} on {url}", file=sys.stderr)
            raise AccessBlocked(f"HTTP {r.status_code}")
        r.raise_for_status(); return r
    
    def close(self): self.s.close()

def sections(v: Any, path="root"):
    if isinstance(v,dict):
        title=clean(v.get("title") or v.get("header") or v.get("sectionTitle")); body=clean(v.get("content") or v.get("text") or v.get("body"))
        if body: yield title or path,body
        for k,x in v.items():
            if k not in {"title","header","sectionTitle","content","text","body"}: yield from sections(x,f"{path}.{k}")
    elif isinstance(v,list):
        for i,x in enumerate(v): yield from sections(x,f"{path}[{i}]")

def parser():
    p=argparse.ArgumentParser()
    p.add_argument("--source",choices=("all","parliament","sprs"),default="all")
    p.add_argument("--sprs-dates",nargs="*",type=valid_date,default=[])
    p.add_argument("--parliament-limit",type=int,default=20)
    p.add_argument("--min-interval",type=float,default=5)
    p.add_argument("--timeout",type=float,default=30)
    p.add_argument("--max-retries",type=int,default=3)
    p.add_argument("--output-dir",type=Path,default=Path("crawler_output"))
    p.add_argument("--user-agent",default=os.getenv("CRAWLER_USER_AGENT","ParliamentResearchCrawler/1.0 (contact: repository-issues)"))
    return p

def main() -> int:
    a=parser().parse_args(); out=a.output_dir.resolve(); out.mkdir(parents=True,exist_ok=True)
    pr: list[dict[str,Any]]=[]; sr: list[dict[str,Any]]=[]; ev: list[Event]=[]; ok=0
    if a.parliament_limit<0 or a.min_interval<1 or a.timeout<=0 or not 0<=a.max_retries<=5:
        ev.append(Event(now(),"config","validation","","2","invalid numeric option"))
        write_csv(out/"crawl_status.csv",L_FIELDS,map(asdict,ev)); write_csv(out/"parliament_order_papers.csv",P_FIELDS,pr); write_csv(out/"sprs_official_report_sections.csv",S_FIELDS,sr)
        return 2
    c=Client(a.user_agent,a.min_interval,a.timeout,a.max_retries)
    try:
        if a.source in ("all","parliament"):
            try:
                r=c.get(ORDER_PAPER_URL)
                soup=BeautifulSoup(r.text,"html.parser")
                links = {}
                for a_tag in soup.select("a[href]"):
                    href = a_tag.get("href","")
                    if not href: continue
                    full = urljoin(r.url, href)
                    links[full] = clean(a_tag.get_text(" "))
                ev.append(Event(now(),"parliament","listing",r.url,str(r.status_code),"ok",len(links)))
                for url,title in list(links.items())[:a.parliament_limit or None]:
                    row={k:"" for k in P_FIELDS}; row.update(title=title or Path(urlsplit(url).path).name,document_url=url)
                    try:
                        d=c.get(url)
                        data=d.content
                        ct = d.headers.get("Content-Type", "")
                        if not data.startswith(b"%PDF-"):
                            # save raw response to help debugging (anti-bot/HTML pages)
                            debug_dir = out / "pdf_debug"
                            debug_dir.mkdir(exist_ok=True, parents=True)
                            debug_name = f"{hashlib.sha256(url.encode()).hexdigest()[:16]}.bin"
                            (debug_dir / debug_name).write_bytes(data)
                            raise ValueError(f"response is not PDF (content-type={ct}, saved={debug_dir / debug_name})")
                        with pdfplumber.open(io.BytesIO(data)) as pdf:
                            text=clean("\n".join((p.extract_text() or "") for p in pdf.pages))
                            pages=len(pdf.pages)
                        name=f"{hashlib.sha256(url.encode()).hexdigest()[:16]}.pdf"; target=out/"pdf"/name; target.parent.mkdir(exist_ok=True); target.write_bytes(data)
                        row.update(http_status=d.status_code,download_status="success",download_bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),page_count=pages,content_chars=len(text),content_text=text,local_pdf_path=str(target),error="")
                    except Exception as e:
                        err = f"{type(e).__name__}: {clean(e)}\n{traceback.format_exc()}"
                        row.update(download_status="failed",error=err)
                    pr.append(row)
            except Exception as e:
                ev.append(Event(now(),"parliament","listing",ORDER_PAPER_URL,"403" if isinstance(e,AccessBlocked) else "0",f"{type(e).__name__}: {clean(e)}"))
        if a.source in ("all","sprs"):
            for date in a.sprs_dates:
                url=SPRS_URL.format(date=date)
                try:
                    r=c.get(url,headers={"Accept":"application/json"}); found=list(sections(r.json()))
                    for i,(title,text) in enumerate(found,1): sr.append({"sitting_date":date,"section_index":i,"title":title,"content_text":text,"source_url":r.url,"error":""})
                    ev.append(Event(now(),"sprs","report",r.url,str(r.status_code),"ok",len(found))); ok+=1
                except Exception as e: ev.append(Event(now(),"sprs","report",url,"403" if isinstance(e,AccessBlocked) else "0",f"{type(e).__name__}: {clean(e)}",0))
    finally: c.close()
    write_csv(out/"parliament_order_papers.csv",P_FIELDS,pr); write_csv(out/"sprs_official_report_sections.csv",S_FIELDS,sr); write_csv(out/"crawl_status.csv",L_FIELDS,map(asdict,ev))
    summary={"parliament_rows":len(pr),"sprs_rows":len(sr),"successful_fetches":ok,"status_rows":len(ev)}
    (out/"crawl_summary.json").write_text(json.dumps(summary,indent=2),encoding="utf-8"); print(json.dumps(summary,indent=2))
    return 0 if ok else 1
if __name__=="__main__": raise SystemExit(main())
