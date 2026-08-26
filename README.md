# Parliament Crawler

## Repository files

- `crawler.py`
- `requirements.txt`
- `.github/workflows/run_crawl.yml`
- `.gitignore`

## GitHub setup

Optionally create the repository secret `CRAWLER_CONTACT`. If it is absent, the workflow uses the repository Issues URL as the public contact.

Run **Actions > Parliament Crawler > Run workflow**. The workflow always uploads `crawler_output`, including logs and status CSV files. Exit code 1 means the crawler ran but the websites did not return usable data; exit code 2 means invalid configuration.

## Local run

```bash
python -m pip install -r requirements.txt
python crawler.py --source all --sprs-dates 08-04-2026 04-03-2026 04-02-2026 --output-dir crawler_output --user-agent "ParliamentResearchCrawler/1.0 (contact: https://github.com/OWNER/REPO/issues)"
```
