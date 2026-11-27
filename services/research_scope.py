"""Resolve calendar, industry and source constraints before library retrieval."""

import re
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlsplit


CHINA_TIME = timezone(timedelta(hours=8))
LATEST = re.compile(r"最新(?:一期|一份|一批|发布)|最近一期|\blatest\b", re.I)
MONTH = re.compile(r"(?:(20\d{2})年)?(1[0-2]|0?[1-9])月(?:以来|起)")
EXPLICIT = re.compile(r"(?<!\d)(20\d{2})[-/年](\d{1,2})[-/月](\d{1,2})(?:日)?(?!\d)")


def _normalized(text):
    return "".join(text.casefold().split())


def _date_range(question, today):
    dates = []
    for match in EXPLICIT.finditer(question):
        try:
            dates.append(date(*map(int, match.groups())))
        except ValueError:
            continue
    if len(dates) >= 2:
        return min(dates), max(dates)
    if dates:
        return dates[0], today if re.search(r"以来|至今|起", question) else dates[0]
    month = MONTH.search(question)
    if month:
        start_month = int(month[2])
        year = int(month[1]) if month[1] else today.year - (start_month > today.month)
        return date(year, start_month, 1), today
    monday = today - timedelta(days=today.weekday())
    if re.search(r"上周|\blast week\b", question, re.I):
        return monday - timedelta(days=7), monday - timedelta(days=1)
    if re.search(r"本周|这周|\bthis week\b", question, re.I):
        return monday, today
    if re.search(r"昨天|昨日|\byesterday\b", question, re.I):
        return (today - timedelta(days=1),) * 2
    if re.search(r"今天|今日|\btoday\b", question, re.I):
        return today, today
    return None


def _record_date(metadata, use_crawl_time):
    if metadata.get("publish_date") and not use_crawl_time:
        return date.fromisoformat(metadata["publish_date"])
    return datetime.fromisoformat(metadata["crawl_time"]).astimezone(CHINA_TIME).date()


def _source_labels(metadata):
    urls = [metadata["source"], metadata["source_url"]]
    hosts = {urlsplit(url).hostname for url in urls}
    labels = {*urls, *(host for host in hosts if host)}
    if any(host and (host == "eastmoney.com" or host.endswith(".eastmoney.com")) for host in hosts):
        labels.update(("东方财富", "eastmoney.com"))
    return {_normalized(label) for label in labels}


def research_scope(question, records, selected, *, today=None):
    """Return a narrowed ID list, or None when the question has no metadata clues."""
    if selected is not None:
        chosen = set(selected)
        records = [(doc_id, meta) for doc_id, meta in records if doc_id in chosen]
        if not records:
            return None
    normalized = _normalized(question)
    time_range = _date_range(question, today or datetime.now(CHINA_TIME).date())
    latest = bool(LATEST.search(question))
    industries = {
        _normalized(industry)
        for _, meta in records
        for industry in meta["industry"]
        if _normalized(industry) in normalized
    }
    explicit_industry = re.findall(r"行业\s*[:：]\s*([^\s，。；,;]+)", question)
    industries.update(_normalized(industry) for industry in explicit_industry)
    sources = {
        label for _, meta in records for label in _source_labels(meta) if label in normalized
    }
    source_filter = re.findall(r"(?:来源|source)\s*[:：]\s*([^\s，。；,;]+)", question, re.I)
    requested_hosts = set(re.findall(r"\b(?:[\w-]+\.)+[a-z]{2,}\b", question.casefold()))
    source_filter.extend(requested_hosts)
    if not any((time_range, latest, industries, sources, source_filter)):
        return None
    use_crawl_time = bool(re.search(r"采集|抓取|归集|\bcrawled\b", question, re.I)) and not bool(
        re.search(r"发布|刊发|\bpublished\b", question, re.I)
    )
    candidates = []
    for doc_id, meta in records:
        if industries and not industries.intersection(
            _normalized(item) for item in meta["industry"]
        ):
            continue
        labels = _source_labels(meta)
        if (sources or source_filter) and not (
            sources.intersection(labels)
            or any(_normalized(term) in label for term in source_filter for label in labels)
        ):
            continue
        day = _record_date(meta, use_crawl_time)
        if not time_range or time_range[0] <= day <= time_range[1]:
            candidates.append((day, doc_id))
    if latest and candidates:
        newest = max(day for day, _ in candidates)
        candidates = [(day, doc_id) for day, doc_id in candidates if day == newest]
    return [doc_id for _, doc_id in candidates]
