"""Discover and fetch PDFs from public report listings and direct links."""

import ipaddress
import json
import re
import socket
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import PurePosixPath
from time import monotonic
from urllib.parse import urlencode, unquote, urljoin, urlsplit

import httpx


EASTMONEY_CENTER = "https://data.eastmoney.com/report/"
DEFAULT_SOURCES = (EASTMONEY_CENTER,)
DATE = re.compile(r"(?<!\d)(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})(?!\d)")
EASTMONEY_HOST = "data.eastmoney.com"
EASTMONEY_LIST = "https://reportapi.eastmoney.com/report/list"
EASTMONEY_PDF = "https://pdf.dfcfw.com/pdf/H3_{}_1.pdf"
EASTMONEY_CALLBACK = "datatable"
EASTMONEY_PAGE_SIZE = 50
EASTMONEY_MAX_PAGES = 3
EASTMONEY_DETAIL = re.compile(r"/report/(?:info/([A-Za-z0-9]+)\.html|zw_\w+\.jshtml)\Z")
EASTMONEY_INITDATA = re.compile(r"var\s+(initdata(?:_\w+)?)\s*=\s*(\{.*?\})\s*;", re.S)
PDF_LABEL = re.compile(r"PDF\s*(?:原文|下载)|(?:查看|下载)\s*PDF|\bdownload pdf\b", re.I)


def source_url(value):
    url = httpx.URL(value)
    port = {"https": 443, "http": 80}.get(url.scheme)
    if not port or not url.host or url.userinfo or url.port not in (None, port):
        raise ValueError("Sources must be public HTTP(S) URLs on their standard ports")
    return str(url.copy_with(fragment=None))


def published(text):
    match = DATE.search(text)
    if match:
        try:
            return date(*map(int, match.groups())).isoformat()
        except ValueError:
            pass
    return None


@dataclass(frozen=True)
class ReportLink:
    url: str
    source: str
    title: str = ""
    date: str | None = None
    referer: str | None = None
    topic: str = ""
    pdf_url: str | None = None


class ReportPage(HTMLParser):
    def __init__(self, html):
        super().__init__()
        self.links = []
        self.anchor = None
        self.title = ""
        self.in_title = False
        self.date = None
        self.feed(html)

    def handle_starttag(self, tag, raw_attributes):
        attrs = dict(raw_attributes)
        if tag == "a" and attrs.get("href"):
            self.anchor = (attrs["href"], attrs.get("title") or "", attrs.get("class") or "")
        elif tag in ("iframe", "embed", "object"):
            url = attrs.get("src") or attrs.get("data")
            if url:
                self.links.append((url, "", attrs.get("type") == "application/pdf"))
        elif tag == "title":
            self.in_title = True
        elif tag == "meta" and (attrs.get("property") or attrs.get("name")) in (
            "article:published_time",
            "pubdate",
            "publishdate",
            "datePublished",
        ):
            self.date = published(attrs.get("content") or "")

    def handle_data(self, data):
        if self.anchor:
            url, label, classes = self.anchor
            self.anchor = (url, label + data, classes)
        if self.in_title:
            self.title += data

    def handle_endtag(self, tag):
        if tag == "a" and self.anchor:
            url, label, classes = self.anchor
            self.links.append((url, " ".join(label.split()), "pdf-link" in classes.split()))
            self.anchor = None
        elif tag == "title":
            self.in_title = False


def matching_industries(text, industries):
    folded = "".join(text.casefold().split())
    return [industry for industry in industries if "".join(industry.casefold().split()) in folded]


class ResearchSources:
    def __init__(self, max_pdf_bytes, check_cancelled):
        self.max_pdf_bytes = max_pdf_bytes
        self.check_cancelled = check_cancelled
        self.client = httpx.Client(
            timeout=8,
            trust_env=False,
            limits=httpx.Limits(max_keepalive_connections=0),
            headers={"User-Agent": "Mozilla/5.0 (compatible; AgentHarness research PDF collector)"},
        )

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.client.close()

    def fetch(self, url, referer=None):
        for attempt in range(2):
            try:
                return self._fetch(url, referer)
            except (httpx.TransportError, httpx.HTTPStatusError) as error:
                if (
                    attempt
                    or isinstance(error, httpx.HTTPStatusError)
                    and (
                        error.response.status_code < 500
                        and error.response.status_code not in (408, 429)
                    )
                ):
                    raise

    def _fetch(self, url, referer):
        deadline = monotonic() + 18
        for _ in range(5):
            self.check_cancelled()
            parsed = httpx.URL(source_url(url))
            host = parsed.raw_host.decode("ascii")
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            ips = [ipaddress.ip_address(entry[4][0]) for entry in addresses]
            if not ips or any(not ip.is_global or ip.is_multicast for ip in ips):
                raise ValueError("Source resolved outside the public internet")
            # Connect to the validated address and keep the source hostname for TLS and HTTP.
            ip = next((address for address in ips if address.version == 4), ips[0])
            headers = {"Host": host}
            if referer:
                headers["Referer"] = referer
            with self.client.stream(
                "GET",
                parsed.copy_with(host=str(ip)),
                headers=headers,
                extensions={"sni_hostname": host},
            ) as response:
                if response.is_redirect and response.headers.get("location"):
                    url = urljoin(str(parsed), response.headers["location"])
                    continue
                response.raise_for_status()
                body = bytearray()
                for block in response.iter_bytes(chunk_size=65536):
                    self.check_cancelled()
                    if monotonic() > deadline:
                        raise httpx.ReadTimeout("Report download exceeded its time budget")
                    body.extend(block)
                    limit = self.max_pdf_bytes if body.startswith(b"%PDF-") else 3 * 1024 * 1024
                    if len(body) > limit:
                        raise ValueError("Source exceeds the document size limit")
                content = (
                    bytes(body)
                    if body.startswith(b"%PDF-")
                    else body.decode(response.encoding or "utf-8", errors="replace")
                )
                return str(parsed), content
        raise ValueError("Too many source redirects")

    @staticmethod
    def _links(page_url, source, page, industries=()):
        parent = urlsplit(page_url)
        report_page = parent.hostname == EASTMONEY_HOST and EASTMONEY_DETAIL.fullmatch(parent.path)
        for href, label, is_pdf in page.links:
            url = urljoin(page_url, href)
            target = urlsplit(url)
            if target.scheme not in ("https", "http") or not target.hostname:
                continue
            direct = is_pdf or target.path.lower().endswith(".pdf") or bool(PDF_LABEL.search(label))
            detail = target.hostname == EASTMONEY_HOST and EASTMONEY_DETAIL.fullmatch(target.path)
            relevant = target.hostname == parent.hostname and matching_industries(label, industries)
            if direct or detail or relevant:
                title = (
                    page.title.split("-研究报告正文", 1)[0].strip()
                    if direct and report_page
                    else label
                )
                yield (
                    ReportLink(url, source, title, published(label) or page.date, page_url),
                    direct,
                )

    def _eastmoney_page(self, source, report_type, page, today):
        query = urlencode(
            {
                "cb": EASTMONEY_CALLBACK,
                "industryCode": "*",
                "industry": "*",
                "rating": "*",
                "ratingChange": "*",
                "code": "*",
                "orgCode": "",
                "rcode": "",
                "fields": "",
                "beginTime": (today - timedelta(days=30)).isoformat(),
                "endTime": today.isoformat(),
                "qType": report_type,
                "pageSize": EASTMONEY_PAGE_SIZE,
                "pageNo": page,
                "p": page,
                "pageNum": page,
            }
        )
        _, body = self.fetch(f"{EASTMONEY_LIST}?{query}", referer=source)
        if not isinstance(body, str):
            raise ValueError("Eastmoney returned an invalid report list")
        body = body.strip().lstrip("\ufeff")
        wrapped = re.fullmatch(rf"{EASTMONEY_CALLBACK}\s*\((.*)\)\s*;?", body, re.S)
        payload = json.loads(wrapped[1] if wrapped else body)
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ValueError("Eastmoney returned an invalid report list")
        return payload["data"]

    @staticmethod
    def _eastmoney_link(row, source, report_type=None):
        if not isinstance(row, dict):
            return None
        code = str(row.get("infoCode") or "")
        if not re.fullmatch(r"[A-Za-z0-9]{1,64}", code):
            return None
        topic = " ".join(
            str(row[key]) for key in ("industryName", "indvInduName", "stockName") if row.get(key)
        )
        detail = (
            f"/report/zw_industry.jshtml?infocode={code}"
            if report_type == 1
            else f"/report/info/{code}.html"
        )
        return ReportLink(
            urljoin(EASTMONEY_CENTER, detail),
            source,
            str(row.get("title") or ""),
            published(str(row.get("publishDate") or "")),
            source,
            topic,
            EASTMONEY_PDF.format(code) if report_type is not None else None,
        )

    def _eastmoney_center(self, source, industries):
        today = datetime.now(timezone(timedelta(hours=8))).date()
        reports = {}
        available = False
        types = [1, 0]  # Industry and stock reports.
        for page in range(1, EASTMONEY_MAX_PAGES + 1):
            for report_type in tuple(types):
                try:
                    rows = self._eastmoney_page(source, report_type, page, today)
                except InterruptedError:
                    raise
                except (httpx.HTTPError, OSError, ValueError, httpx.InvalidURL):
                    types.remove(report_type)
                    continue
                available = True
                if len(rows) < EASTMONEY_PAGE_SIZE:
                    types.remove(report_type)
                for row in rows:
                    link = self._eastmoney_link(row, source, report_type)
                    if link and matching_industries(
                        f"{link.title} {link.topic}",
                        industries,
                    ):
                        reports.setdefault(link.pdf_url, link)
            if not types:
                break
        return list(reports.values()), available

    def discover(self, sources, industries):
        candidates = []
        failed = 0
        for source in sources:
            try:
                location = urlsplit(source)
                center = (
                    location.hostname == EASTMONEY_HOST and location.path.rstrip("/") == "/report"
                )
                if center:
                    reports, api_available = self._eastmoney_center(source, industries)
                    if reports:
                        candidates.extend((link, None, False) for link in reports)
                        continue
                url, content = self.fetch(source)
                if isinstance(content, bytes):
                    candidates.append((ReportLink(url, source), content, True))
                    continue
                before = len(candidates)
                embedded_list = False
                if urlsplit(url).hostname == EASTMONEY_HOST:
                    for name, raw in EASTMONEY_INITDATA.findall(content):
                        try:
                            payload = json.loads(raw)
                        except ValueError:
                            continue
                        rows = payload.get("data") if isinstance(payload, dict) else None
                        if not isinstance(rows, list):
                            continue
                        embedded_list = True
                        report_type = (
                            1 if name.startswith("initdata_industry") else 0 if center else None
                        )
                        for row in rows:
                            link = self._eastmoney_link(row, source, report_type)
                            if link and (
                                not center
                                or matching_industries(
                                    f"{link.title} {link.topic}",
                                    industries,
                                )
                            ):
                                candidates.append((link, None, False))
                candidates.extend(
                    (link, None, direct)
                    for link, direct in self._links(
                        url,
                        source,
                        ReportPage(content),
                        () if center else industries,
                    )
                    if link.url != url
                )
                if center and not (api_available or embedded_list or len(candidates) > before):
                    failed += 1
            except InterruptedError:
                raise
            except (httpx.HTTPError, OSError, ValueError, httpx.InvalidURL):
                failed += 1
        candidates.sort(
            key=lambda candidate: (
                len(matching_industries(candidate[0].title + " " + candidate[0].topic, industries)),
                candidate[0].date or "",
            ),
            reverse=True,
        )
        unique = {}
        for candidate in candidates:
            unique.setdefault(candidate[0].pdf_url or candidate[0].url, candidate)
        return list(unique.values())[:25], failed

    def download(self, link, content, direct):
        if content is not None:
            return link.url, content, link.title, link.date
        if link.pdf_url:
            try:
                url, content = self.fetch(link.pdf_url, referer=link.referer)
                if isinstance(content, bytes):
                    return url, content, link.title, link.date
            except InterruptedError:
                raise
            except (httpx.HTTPError, OSError, ValueError, httpx.InvalidURL):
                pass  # Try the provider's report detail page for the PDF link.
        routes = [link.url]
        path = urlsplit(link.url)
        match = EASTMONEY_DETAIL.fullmatch(path.path) if path.hostname == EASTMONEY_HOST else None
        if match and match[1]:
            routes.append(urljoin(link.url, f"/report/zw_industry.jshtml?infocode={match[1]}"))
        for route in routes:
            try:
                url, content = self.fetch(route, referer=link.referer)
            except httpx.HTTPStatusError as error:
                if route != routes[-1] and error.response.status_code in (403, 404):
                    continue
                raise
            if isinstance(content, bytes):
                return url, content, link.title, link.date
            if direct:
                break
            page = ReportPage(content)
            pdfs = dict.fromkeys(
                pdf.url
                for pdf, is_pdf in self._links(url, link.source, page)
                if is_pdf and pdf.url != url
            )
            for pdf in list(pdfs)[:3]:
                try:
                    pdf_url, body = self.fetch(pdf, referer=url)
                except InterruptedError:
                    raise
                except (httpx.HTTPError, OSError, ValueError, httpx.InvalidURL):
                    continue
                if isinstance(body, bytes):
                    title = page.title.split("-研究报告正文", 1)[0].strip()
                    return pdf_url, body, link.title or title, link.date or page.date
        raise ValueError("Report page does not link to a readable PDF")

    @staticmethod
    def fallback_title(url):
        return unquote(PurePosixPath(urlsplit(url).path).stem) or "Report"
