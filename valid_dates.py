#!/usr/bin/env python3
"""
Fetch program pages as Markdown (via Jina Reader) and extract 2026 application deadlines using Gemini.

Input:  ec_bank_rows_with_valid_url.csv
Output: results.json

Behavior:
- Only processes rows where valid_url == "TRUE"
- Converts webpage to Markdown via Jina Reader (r.jina.ai) and stores it under ./md-files/
- Sends Markdown to Gemini (model: gemini-3-flash-preview)
- Prompts Gemini to extract ONLY 2026 application deadline(s)
- Writes cumulative JSON results to results.json (incremental; safe to resume)

Requires:
- GEMINI_API_KEY in environment OR in a local file `.env.local` (key=value lines)
"""

from __future__ import annotations

import argparse
import csv
import gzip
import html
import json
import os
import re
import random
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None  # type: ignore


MODEL = "gemini-3-flash-preview"

TRUE_STR = "TRUE"

OPTIONS_PY_PATH = "options.py"


@dataclass(frozen=True)
class FetchConfig:
    timeout_seconds: float
    user_agent: str
    retries: int
    backoff_seconds: float
    min_domain_interval_seconds: float


@dataclass(frozen=True)
class GeminiConfig:
    api_key: str
    model: str
    timeout_seconds: float
    max_output_tokens: int
    retries: int
    backoff_seconds: float


@dataclass(frozen=True)
class JinaConfig:
    timeout_seconds: float
    retries: int
    backoff_seconds: float


def load_env_local(env_path: str = ".env.local") -> None:
    """
    Minimal .env loader for KEY=VALUE lines. Does not override existing env vars.
    """
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        # Keep script resilient; user can also export GEMINI_API_KEY manually.
        return


def _read_csv_rows(path: str) -> Tuple[List[str], List[dict]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV has no header row.")
        fieldnames = list(reader.fieldnames)
        rows = list(reader)
    return fieldnames, rows


def _write_csv_rows(path: str, fieldnames: List[str], rows: Iterable[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _normalize_url(raw: str) -> Optional[str]:
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None
    parsed = urllib.parse.urlparse(s)
    if not parsed.scheme:
        s = "https://" + s
        parsed = urllib.parse.urlparse(s)
    if parsed.scheme not in ("http", "https"):
        return None
    if not parsed.netloc:
        return None
    return s


def _parse_retry_after_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    v = value.strip()
    if not v:
        return None
    # Could be delta-seconds or an HTTP date.
    try:
        return float(v)
    except Exception:
        pass
    try:
        dt = parsedate_to_datetime(v)
        return max(0.0, dt.timestamp() - time.time())
    except Exception:
        return None


class DomainRateLimiter:
    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval_seconds = max(0.0, float(min_interval_seconds))
        self._last_request_by_netloc: Dict[str, float] = {}

    def wait(self, url: str) -> None:
        if self.min_interval_seconds <= 0:
            return
        try:
            netloc = urllib.parse.urlparse(url).netloc.lower()
        except Exception:
            return
        if not netloc:
            return
        now = time.time()
        last = self._last_request_by_netloc.get(netloc)
        if last is not None:
            delta = now - last
            if delta < self.min_interval_seconds:
                time.sleep(self.min_interval_seconds - delta)
        self._last_request_by_netloc[netloc] = time.time()


def _sleep_backoff(base_seconds: float, attempt: int) -> None:
    base = max(0.0, float(base_seconds))
    # Exponential backoff with a bit of jitter.
    delay = base * (2 ** max(0, attempt))
    delay = delay * (0.8 + random.random() * 0.4)
    if delay > 0:
        time.sleep(delay)


def _fetch_bytes(url: str, timeout_seconds: float, headers: Dict[str, str]) -> Tuple[bytes, Dict[str, str]]:
    req = urllib.request.Request(url=url, method="GET", headers=headers)
    with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
        raw_bytes = resp.read()
        resp_headers = {k: v for k, v in resp.headers.items()}
        return raw_bytes, resp_headers


def fetch_html(url: str, cfg: FetchConfig, limiter: DomainRateLimiter) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        raise ValueError("Invalid URL")

    headers = {
        "User-Agent": cfg.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Accept-Encoding": "gzip",
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
    }

    last_exc: Optional[BaseException] = None
    for attempt in range(cfg.retries + 1):
        limiter.wait(normalized)
        if attempt > 0:
            _sleep_backoff(cfg.backoff_seconds, attempt)

        try:
            raw_bytes, resp_headers = _fetch_bytes(normalized, cfg.timeout_seconds, headers)
            charset = "utf-8"
            content_type = resp_headers.get("Content-Type", "")
            m = re.search(r"charset=([^\s;]+)", content_type, re.IGNORECASE)
            if m:
                charset = m.group(1).strip().strip('"').strip("'")

            encoding = (resp_headers.get("Content-Encoding") or "").lower().strip()
            if "gzip" in encoding:
                try:
                    raw_bytes = gzip.decompress(raw_bytes)
                except Exception:
                    pass

            try:
                return raw_bytes.decode(charset, errors="replace")
            except Exception:
                return raw_bytes.decode("utf-8", errors="replace")

        except urllib.error.HTTPError as e:
            # Respect 429 Retry-After; treat 5xx/429 as retryable.
            last_exc = e
            status = getattr(e, "code", None)
            if status == 429:
                retry_after = _parse_retry_after_seconds(e.headers.get("Retry-After"))
                if retry_after is not None and retry_after > 0:
                    time.sleep(retry_after)
                continue
            if status is not None and int(status) >= 500:
                continue
            raise
        except (urllib.error.URLError, socket.timeout, TimeoutError) as e:
            last_exc = e
            continue

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Failed to fetch HTML")

def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    half = max_chars // 2
    head = text[:half]
    tail = text[-half:]
    return head + "\n\n... [TRUNCATED] ...\n\n" + tail


def _safe_slug(s: str, max_len: int = 80) -> str:
    s2 = re.sub(r"[^a-zA-Z0-9]+", "-", s.strip()).strip("-").lower()
    if not s2:
        s2 = "item"
    return s2[:max_len]


def _has_date_signal(text: str) -> bool:
    """
    Heuristic: does the text contain any date-like signal?
    Used to detect when a markdown extractor has dropped a critical "Dates" section.
    """
    if not text:
        return False
    if re.search(r"\b20\d{2}\b", text):
        return True
    # Month-name detection must be tied to a day number to avoid false positives
    # (e.g., "may" as a verb).
    if re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b\s+\d{1,2}\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if re.search(
        r"\b\d{1,2}\b\s+\b(January|February|March|April|May|June|July|August|September|October|November|December)\b",
        text,
        re.IGNORECASE,
    ):
        return True
    if re.search(r"\b\d{2}-\d{2}-\d{4}\b", text):
        return True
    return False


class _HTMLToText(HTMLParser):
    """
    Minimal HTML->text conversion (no external deps). Keeps basic structure.
    """

    def __init__(self) -> None:
        super().__init__()
        self._chunks: List[str] = []
        self._skip_depth = 0  # inside <script>/<style>/<noscript>

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        t = tag.lower()
        if t in ("script", "style", "noscript"):
            self._skip_depth += 1
            return
        if self._skip_depth > 0:
            return
        if t in ("br",):
            self._chunks.append("\n")
        if t in ("p", "div", "section", "article", "header", "footer", "h1", "h2", "h3", "h4", "h5", "h6", "li"):
            self._chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in ("script", "style", "noscript"):
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth > 0:
            return
        if t in ("p", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        txt = html.unescape(data)
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt:
            self._chunks.append(txt + " ")

    def get_text(self) -> str:
        out = "".join(self._chunks)
        # normalize whitespace/newlines
        out = re.sub(r"[ \t]+\n", "\n", out)
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()


def html_to_text_basic(full_html: str) -> str:
    parser = _HTMLToText()
    parser.feed(full_html)
    return parser.get_text()


def fetch_markdown_via_jina(url: str, cfg: JinaConfig, limiter: DomainRateLimiter) -> str:
    """
    Uses Jina Reader to convert a page to a text/markdown-like representation.
    """
    normalized = _normalize_url(url)
    if not normalized:
        raise ValueError("Invalid URL")

    # Jina reader endpoint pattern: https://r.jina.ai/http(s)://example.com/...
    jina_url = "https://r.jina.ai/" + normalized
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; college-craft-jina-reader/1.0)",
        "Accept": "text/plain,*/*",
        "Connection": "close",
    }

    last_exc: Optional[BaseException] = None
    for attempt in range(cfg.retries + 1):
        limiter.wait(jina_url)
        if attempt > 0:
            _sleep_backoff(cfg.backoff_seconds, attempt)
        try:
            raw_bytes, resp_headers = _fetch_bytes(jina_url, cfg.timeout_seconds, headers)
            charset = "utf-8"
            content_type = resp_headers.get("Content-Type", "")
            m = re.search(r"charset=([^\s;]+)", content_type, re.IGNORECASE)
            if m:
                charset = m.group(1).strip().strip('"').strip("'")
            return raw_bytes.decode(charset, errors="replace")
        except urllib.error.HTTPError as e:
            last_exc = e
            status = getattr(e, "code", None)
            if status == 429:
                retry_after = _parse_retry_after_seconds(e.headers.get("Retry-After"))
                if retry_after is not None and retry_after > 0:
                    time.sleep(retry_after)
                continue
            if status is not None and int(status) >= 500:
                continue
            raise
        except (urllib.error.URLError, socket.timeout, TimeoutError) as e:
            last_exc = e
            continue

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Failed to fetch markdown via Jina")


def fetch_markdown_with_fallback(
    url: str,
    jina_cfg: JinaConfig,
    fetch_cfg: FetchConfig,
    limiter: DomainRateLimiter,
) -> str:
    """
    Primary: Jina Reader markdown.
    Fallback/augmentation: optionally append a full HTML->text extraction to be more "full page".
    This addresses cases where r.jina.ai drops important teaser/summary blocks or only captures
    a fraction of the visible page.
    """
    md = fetch_markdown_via_jina(url, jina_cfg, limiter)
    return md


def fetch_markdown_full_page(
    url: str,
    jina_cfg: JinaConfig,
    fetch_cfg: FetchConfig,
    limiter: DomainRateLimiter,
    *,
    always_augment_html: bool,
    min_md_chars_for_no_augment: int,
) -> str:
    """
    Returns markdown that is more likely to represent the "whole page".

    We start with Jina Reader output (often cleaner), then optionally append a basic full-page
    HTML->text extraction (no external deps).
    """
    md = fetch_markdown_with_fallback(url, jina_cfg, fetch_cfg, limiter)

    should_augment = always_augment_html
    if not should_augment and min_md_chars_for_no_augment > 0:
        if len(md) < int(min_md_chars_for_no_augment):
            should_augment = True
    if not should_augment and not _has_date_signal(md):
        should_augment = True

    if not should_augment:
        return md

    html_full = fetch_html(url, fetch_cfg, limiter)
    html_text = html_to_text_basic(html_full)

    combined = (
        md
        + "\n\n---\n\n"
        + "HTML Extracted Text (full-page augmentation):\n\n"
        + html_text
    )
    return combined


def load_options_from_options_py(path: str = OPTIONS_PY_PATH) -> Dict[str, Dict[str, Any]]:
    """
    Reads the repo's `options.py` (which is not necessarily valid Python) and extracts:
    - multi_select: bool
    - options: [str]

    Expected rough format:
      field = {
        multi-select: True/False,
        options: ["A", "B", ...]
      }
    """
    if not os.path.exists(path):
        return {}
    try:
        raw = open(path, "r", encoding="utf-8", errors="replace").read()
    except Exception:
        return {}

    # Split into blocks by "<name> = {"
    blocks: Dict[str, str] = {}
    for m in re.finditer(r"(?m)^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*\{\s*$", raw):
        name = m.group(1)
        start = m.end()
        # Find the next "}\n" after start (naive but works for this small file).
        end_m = re.search(r"(?m)^\s*\}\s*$", raw[start:])
        if not end_m:
            continue
        end = start + end_m.start()
        blocks[name] = raw[start:end]

    out: Dict[str, Dict[str, Any]] = {}
    for name, body in blocks.items():
        ms = None
        ms_m = re.search(r"(?i)multi-select\s*:\s*(true|false)", body)
        if ms_m:
            ms = ms_m.group(1).lower() == "true"
        opts = re.findall(r"\"([^\"]+)\"", body)
        out[name] = {"multi_select": bool(ms) if ms is not None else False, "options": opts}
    return out


def extract_markdown_links(md_text: str, base_url: str) -> List[Dict[str, str]]:
    """
    Extracts links from markdown in the form [text](url) and returns a list of:
      { "text": "...", "url": "https://..." }
    """
    links: List[Dict[str, str]] = []
    seen: set[str] = set()

    for m in re.finditer(r"\[([^\]]+)\]\(([^)]+)\)", md_text):
        text = (m.group(1) or "").strip()
        href = (m.group(2) or "").strip()
        if not href or href.startswith("#"):
            continue
        href = href.split("#", 1)[0].strip()
        abs_url = urllib.parse.urljoin(base_url, href)
        abs_url = _normalize_url(abs_url) or ""
        if not abs_url:
            continue
        if abs_url in seen:
            continue
        seen.add(abs_url)
        links.append({"text": text, "url": abs_url})

    return links


def extract_html_links_basic(full_html: str, base_url: str, limit: int = 200) -> List[Dict[str, str]]:
    """
    Very lightweight href extraction as a fallback if markdown contains few/no links.
    Returns {text, url} where text may be empty.
    """
    links: List[Dict[str, str]] = []
    seen: set[str] = set()

    # Extract hrefs. We won't attempt perfect HTML parsing; just a simple regex.
    for m in re.finditer(r'href\s*=\s*["\']([^"\']+)["\']', full_html, re.IGNORECASE):
        href = (m.group(1) or "").strip()
        if not href or href.startswith("#"):
            continue
        href = href.split("#", 1)[0].strip()
        abs_url = urllib.parse.urljoin(base_url, href)
        abs_url = _normalize_url(abs_url) or ""
        if not abs_url or abs_url in seen:
            continue
        seen.add(abs_url)
        links.append({"text": "", "url": abs_url})
        if len(links) >= limit:
            break
    return links


class _HTMLLinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_a = False
        self._href: Optional[str] = None
        self._text_chunks: List[str] = []
        self.links: List[Dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        if tag.lower() != "a":
            return
        href = None
        for k, v in attrs:
            if str(k).lower() == "href":
                href = "" if v is None else str(v)
                break
        if href is None:
            return
        href = href.strip()
        if not href or href.startswith("#"):
            return
        if href.lower().startswith(("mailto:", "tel:", "javascript:")):
            return
        self._in_a = True
        self._href = href
        self._text_chunks = []

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a":
            return
        if not self._in_a:
            return
        text = re.sub(r"\s+", " ", "".join(self._text_chunks)).strip()
        href = (self._href or "").strip()
        if href:
            self.links.append({"text": text, "url": href})
        self._in_a = False
        self._href = None
        self._text_chunks = []

    def handle_data(self, data: str) -> None:
        if not self._in_a:
            return
        if data:
            self._text_chunks.append(data)


def extract_html_links(full_html: str, base_url: str, limit: int = 400) -> List[Dict[str, str]]:
    """
    Extract (anchor text, href) from HTML and normalize to absolute URLs.
    """
    try:
        parser = _HTMLLinkExtractor()
        parser.feed(full_html)
        raw_links = parser.links
    except Exception:
        raw_links = []

    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for it in raw_links:
        href = (it.get("url") or "").strip()
        text = (it.get("text") or "").strip()
        if not href or href.startswith("#"):
            continue
        href = href.split("#", 1)[0].strip()
        abs_url = urllib.parse.urljoin(base_url, href)
        abs_url = _normalize_url(abs_url) or ""
        if not abs_url or abs_url in seen:
            continue
        seen.add(abs_url)
        out.append({"text": text, "url": abs_url})
        if len(out) >= limit:
            break
    return out


def _score_link_candidate(text: str, url: str) -> int:
    """
    Heuristic scoring to surface likely "apply / deadline / dates" pages.
    """
    s = 0
    hay = f"{text} {url}".lower()
    for kw in (
        "apply",
        "application",
        "deadline",
        "dates",
        "session",
        "schedule",
        "calendar",
        "tuition",
        "cost",
        "fees",
        "eligibility",
        "requirements",
        "admissions",
        "how to apply",
    ):
        if kw in hay:
            s += 5
    if "pdf" in hay:
        s -= 2
    return s


def choose_links_heuristic(
    candidates: List[Dict[str, str]],
    base_url: str,
    max_links: int,
) -> List[Dict[str, str]]:
    """
    If Gemini selects nothing (or link text is sparse), fall back to heuristic picks.
    """
    scored = []
    for c in candidates:
        u = (c.get("url") or "").strip()
        if not u:
            continue
        # Avoid self / obvious social links
        if u.rstrip("/") == (base_url or "").rstrip("/"):
            continue
        score = _score_link_candidate(c.get("text", ""), u)
        if score <= 0:
            continue
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    out: List[Dict[str, str]] = []
    for _s, c in scored:
        if len(out) >= max_links:
            break
        out.append({"url": c.get("url", ""), "label": c.get("text", "")})
    return out


def choose_additional_links_with_gemini(
    program_name: str,
    main_md: str,
    base_url: str,
    candidates: List[Dict[str, str]],
    cfg: GeminiConfig,
    max_links: int,
) -> List[Dict[str, str]]:
    """
    Round 1: Ask Gemini to choose up to `max_links` additional pages worth crawling.
    Returns a list of {url, label}.
    """
    # Keep candidate list compact but high-signal.
    scored = sorted(
        candidates,
        key=lambda x: _score_link_candidate(x.get("text", ""), x.get("url", "")),
        reverse=True,
    )
    top = scored[:60]

    # Keep main markdown limited; Gemini only needs enough context to decide what to click.
    main_snip = _truncate_text(main_md, 30000)
    candidate_lines = "\n".join(
        [f"- text: {c.get('text','')}\n  url: {c.get('url','')}" for c in top]
    )
    prompt = f"""You are helping crawl a program webpage to find key application and session details.

Program name: "{program_name}"
Main page URL: {base_url}

Task:
Choose up to {max_links} additional links from the candidate list that are MOST likely to contain:
- application deadlines / how-to-apply / admissions
- program/session dates
- eligibility / grade level requirements
- cost/tuition/fees

Return ONLY valid JSON (no markdown/code fences) with this schema:
{{
  "selected": [
    {{"url": "https://...", "label": "why this page"}}
  ]
}}

Constraints:
- Only choose URLs from the candidate list.
- Prefer same-site, relevant pages. Avoid obvious navigation duplicates (home, search, donate, etc.).
- If the main page already contains everything, return an empty list.

MAIN PAGE MARKDOWN (snippet):
{main_snip}

CANDIDATE LINKS:
{candidate_lines}
"""
    schema_hint = """{"selected":[{"url":"https://...","label":"why this page"}]}"""
    raw = _gemini_generate_content(prompt, cfg, response_mime_type="application/json")
    try:
        obj = _extract_json_object(raw)
    except Exception:
        obj = _repair_json_with_gemini(raw, schema_hint=schema_hint, cfg=cfg)
    selected = obj.get("selected", [])
    if not isinstance(selected, list):
        return []

    out: List[Dict[str, str]] = []
    allowed = {c["url"] for c in top if isinstance(c.get("url"), str)}
    for item in selected[: max(0, int(max_links))]:
        if not isinstance(item, dict):
            continue
        u = item.get("url")
        if not isinstance(u, str):
            continue
        u = u.strip().split("#", 1)[0]
        if u not in allowed:
            continue
        label = item.get("label")
        out.append({"url": u, "label": "" if label is None else str(label).strip()})
    return out


def _dedupe_list_of_dicts(items: List[dict], key_fields: Tuple[str, ...]) -> List[dict]:
    seen: set[tuple] = set()
    out: List[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        k = tuple((it.get(f) or "") for f in key_fields)
        if k in seen:
            continue
        seen.add(k)
        out.append(it)
    return out


def _dedupe_str_list(items: List[str]) -> List[str]:
    out: List[str] = []
    for x in items:
        s = "" if x is None else str(x).strip()
        if not s:
            continue
        if s not in out:
            out.append(s)
    return out


_MONTHS = {
    "january": "01",
    "february": "02",
    "march": "03",
    "april": "04",
    "may": "05",
    "june": "06",
    "july": "07",
    "august": "08",
    "september": "09",
    "october": "10",
    "november": "11",
    "december": "12",
}


def _month_name_date_to_mmddyyyy(s: str) -> Optional[str]:
    """
    Converts 'March 1, 2026' -> '03-01-2026' when possible.
    """
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\b\s+(\d{1,2}),\s*(20\d{2})\b",
        s,
        re.IGNORECASE,
    )
    if not m:
        return None
    mon = _MONTHS.get(m.group(1).lower())
    if not mon:
        return None
    day = int(m.group(2))
    year = m.group(3)
    return f"{mon}-{day:02d}-{year}"


def _infer_grade_levels(text: str) -> List[str]:
    """
    Pull grade levels from clear signals like:
    - entering 9th-12th
    - entering 10th grade and older
    - rising junior and senior
    """
    t = (text or "").lower()
    out: List[str] = []

    def add(g: str) -> None:
        if g not in out:
            out.append(g)

    # Range "9th-12th" / "9-12" / "9–12"
    if re.search(r"\b(entering|for)\s+9(?:th)?\s*[-–]\s*12(?:th)?\b", t):
        for g in ("9th", "10th", "11th", "12th"):
            add(g)

    # Explicit grade mentions
    for g in ("9th", "10th", "11th", "12th"):
        if re.search(rf"\b{re.escape(g)}\b", t) or re.search(rf"\b{g[:-2]}\s*(?:th)?\s*grade\b", t):
            add(g)

    # Rising class years
    if "rising freshman" in t or "rising 9" in t:
        add("9th")
    if "rising sophomore" in t or "rising 10" in t:
        add("10th")
    if "rising junior" in t or "rising 11" in t:
        add("11th")
    if "rising senior" in t or "rising 12" in t:
        add("12th")

    return out


def _infer_eligibility(text: str) -> List[str]:
    t = (text or "").lower()
    out: List[str] = []
    if re.search(r"\binternational student(s)?\b", t) or re.search(r"\binternational applicants?\b", t):
        out.append("International Students")
    if re.search(r"\bdomestic student(s)?\b", t) or re.search(r"\bus student(s)?\b", t) or re.search(r"\bdomestic applicants?\b", t):
        out.append("Domestic Students")
    return out


def extract_deadlines_rule_based(combined_md: str) -> List[dict]:
    """
    Deterministically extract deadline-like rows from markdown text.
    Especially important for common 'Application Dates & Deadlines' tables.
    """
    text = combined_md or ""
    deadlines: List[dict] = []

    # Table rows like: | March 1, 2026 | **International Student Application Deadline** |
    for line in text.splitlines():
        if "|" not in line:
            continue
        if not re.search(r"20\d{2}", line):
            continue
        date = _month_name_date_to_mmddyyyy(line)
        if not date:
            continue
        # Pull a label from bold **...** or from the remainder of the row.
        label = ""
        m = re.search(r"\*\*(.+?)\*\*", line)
        if m:
            label = m.group(1).strip()
        else:
            # crude: take last column text
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if len(parts) >= 2:
                candidate_label = parts[-1]
                # Ignore if it looks like a time (e.g., "4-8 p.m.")
                if re.search(r"\d{1,2}(?::\d{2})?\s*(?:a\.m\.|p\.m\.|am|pm)", candidate_label, re.IGNORECASE):
                    continue
                label = candidate_label

        if not label:
            continue
        deadlines.append({"label": label, "date": date})

    # Sentence patterns like "deadline for international students ... March 1, 2026"
    for m in re.finditer(r"deadline[^.\n]{0,200}(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s*20\d{2}", text, re.IGNORECASE):
        snippet = m.group(0)
        date = _month_name_date_to_mmddyyyy(snippet)
        if not date:
            continue
        
        # Filter out snippets that look like event times rather than deadlines
        if re.search(r"\d{1,2}(?::\d{2})?\s*(?:a\.m\.|p\.m\.|am|pm)", snippet, re.IGNORECASE):
            # Only skip if the snippet doesn't also contain a strong deadline keyword
            if not re.search(r"application|admission|priority|final|deadline", snippet, re.IGNORECASE):
                continue

        label = "Application Deadline"
        low = snippet.lower()
        if "international" in low:
            label = "International Student Application Deadline"
        elif "domestic" in low:
            label = "Domestic Student Application Deadline"
        deadlines.append({"label": label, "date": date})

    return _dedupe_list_of_dicts(deadlines, ("label", "date"))


def extract_all_data_unified(
    program_name: str,
    combined_md: str,
    options: Dict[str, Dict[str, Any]],
    cfg: GeminiConfig,
    max_chars: int,
) -> Dict[str, Any]:
    """
    Unified extraction of all program data in a single high-accuracy pass.
    Uses Chain-of-Thought (via 'thought' field) and strict schema enforcement.
    """
    md = _truncate_text(combined_md, max_chars)

    def fmt_opts(key: str) -> str:
        opts = options.get(key, {}).get("options", [])
        if not isinstance(opts, list):
            return "[]"
        return json.dumps(opts, ensure_ascii=False)

    prompt = f"""You are an expert data extraction agent. Your task is to extract highly accurate, structured information about an extracurricular program from the provided text.

Program Name: "{program_name}"

Return ONLY valid JSON (no markdown/code fences) with exactly this schema:
{{
  "thought": "Briefly explain your reasoning for each field below based on specific evidence in the text.",
  "deadlines": [
    {{"label": "early|regular|final|priority|rolling|...", "date": "MM-DD-YYYY"}}
  ],
  "program_dates": [
    {{"label": "Session 1|Session I|Summer Session|...", "dates": ["MM-DD-YYYY", "MM-DD-YYYY"]}}
  ],
  "mode": "In-Person|Remote|Hybrid",
  "price": "Free|Paid",
  "eligibility": ["Domestic Students", "International Students"],
  "grade_level": ["9th", "10th", "11th", "12th"],
  "location": ["All States", "International", "Alabama", "..."],
  "details_tags": ["Architecture", "Art", "STEM", "Medicine", "..."],
  "details": "One paragraph describing who/what/logistics. Omit location and date/time."
}}

ALLOWED VALUES (Must choose ONLY from these options or return empty string/list):
- mode: {fmt_opts("mode")}
- price: {fmt_opts("price")}
- eligibility: {fmt_opts("eligibility")}
- grade_level: {fmt_opts("grade_level")}
- location: {fmt_opts("location")}
- details_tags: {fmt_opts("details")}

EXTRACTION RULES:
1. ACCURACY IS PARAMOUNT. Do not invent facts.
2. DATES: Use MM-DD-YYYY format. For program_dates, use ["MM-DD-YYYY", ""] if only start is known, or ["", "MM-DD-YYYY"] if only end is known.
3. YEARS: Focus on 2026 dates. If 2026 is unavailable, use 2025. 
4. DEADLINES: Include all application-related deadlines (international, domestic, financial aid, etc.).
5. GRADE LEVEL: Infer from "rising junior", "10th grade", "ages 14-18", etc.
6. LOCATION: Choose all applicable states or "International"/"All States".
7. DETAILS: Exactly one cohesive paragraph, at least 3 sentences. Capture the essence of the program (who/what/logistics). Do not include specific dates or locations in the paragraph.
8. MODE: Choose "In-Person" if it mentions being residential, on-campus, or at a specific physical location. Choose "Remote" if it's online or virtual.
9. PRICE: Choose "Paid" if there is a tuition, fee, or cost mentioned. Choose "Free" only if it explicitly states there is no cost.

TEXT TO PROCESS:
{md}
"""
    schema_hint = """{"thought":"","deadlines":[],"program_dates":[],"mode":"","price":"","eligibility":[],"grade_level":[],"location":[],"details_tags":[],"details":""}"""
    
    # Use responseMimeType="application/json" for better reliability
    raw = _gemini_generate_content(prompt, cfg, response_mime_type="application/json")
    try:
        obj = _extract_json_object(raw)
    except Exception:
        obj = _repair_json_with_gemini(raw, schema_hint=schema_hint, cfg=cfg)
    
    if not isinstance(obj, dict):
        return {}
    
    # Log the thought process if verbose
    thought = obj.get("thought")
    if thought:
        # We don't want to print to stdout to avoid messing up output, 
        # but stderr is fine for debugging if we were in a terminal.
        pass

    return obj


def _gemini_generate_content(prompt: str, cfg: GeminiConfig, response_mime_type: Optional[str]) -> str:
    endpoint = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{urllib.parse.quote(cfg.model)}:generateContent?key={urllib.parse.quote(cfg.api_key)}"
    )

    generation_config: Dict[str, Any] = {
        "temperature": 0.0,
        "maxOutputTokens": int(cfg.max_output_tokens),
    }
    if response_mime_type:
        generation_config["responseMimeType"] = response_mime_type

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": generation_config,
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        endpoint,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )

    last_exc: Optional[BaseException] = None
    for attempt in range(cfg.retries + 1):
        if attempt > 0:
            _sleep_backoff(cfg.backoff_seconds, attempt)
        try:
            with urllib.request.urlopen(req, timeout=cfg.timeout_seconds) as resp:
                resp_bytes = resp.read()
                resp_text = resp_bytes.decode("utf-8", errors="replace")
                resp_json = json.loads(resp_text)
            break
        except urllib.error.HTTPError as e:
            last_exc = e
            status = getattr(e, "code", None)
            if status in (429, 500, 502, 503, 504):
                retry_after = _parse_retry_after_seconds(getattr(e, "headers", {}).get("Retry-After"))
                if retry_after is not None and retry_after > 0:
                    time.sleep(retry_after)
                continue
            raise
        except (urllib.error.URLError, socket.timeout, TimeoutError) as e:
            last_exc = e
            continue
    else:
        raise last_exc or RuntimeError("Gemini request failed.")

    candidates = resp_json.get("candidates") or []
    if not candidates:
        raise RuntimeError("Gemini returned no candidates.")
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    if not parts:
        raise RuntimeError("Gemini returned no content parts.")
    text = parts[0].get("text")
    if not isinstance(text, str):
        raise RuntimeError("Gemini response missing text.")
    return text


def _extract_json_object(text: str) -> Dict[str, Any]:
    """
    Gemini sometimes wraps JSON in code fences. This extracts the first JSON object.
    """
    s = text.strip()
    if s.startswith("```"):
        # strip triple-backtick fences
        s = re.sub(r"^```[a-zA-Z0-9_-]*\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
        s = s.strip()

    # First: try parse as-is.
    try:
        obj0 = json.loads(s)
        if isinstance(obj0, dict):
            return obj0
    except Exception:
        pass

    # Fallback: find first JSON object using the standard decoder (robust to leading/trailing text).
    decoder = json.JSONDecoder()
    start = s.find("{")
    if start == -1:
        raise ValueError("No JSON object found in Gemini output.")
    try:
        obj, _end = decoder.raw_decode(s[start:])
    except Exception as e:
        raise ValueError(f"Failed to parse JSON object from Gemini output: {e}") from e
    if not isinstance(obj, dict):
        raise ValueError("Expected a JSON object.")
    return obj


def _repair_json_with_gemini(
    bad_text: str,
    schema_hint: str,
    cfg: GeminiConfig,
) -> Dict[str, Any]:
    """
    Best-effort: ask Gemini to rewrite malformed JSON into valid JSON following schema_hint.
    """
    prompt = f"""You are a JSON repair tool.

Your task: rewrite the content below into STRICTLY valid JSON that follows this schema:
{schema_hint}

Rules:
- Output ONLY valid JSON.
- Do not include markdown fences or any commentary.
- If a field is unknown, use an empty string, empty list, or empty object as appropriate.

CONTENT TO REPAIR:
{bad_text}
"""
    repaired = _gemini_generate_content(prompt, cfg, response_mime_type="application/json")
    return _extract_json_object(repaired)


def _is_mm_dd_yyyy(date_str: str) -> bool:
    return bool(re.fullmatch(r"\d{2}-\d{2}-\d{4}", date_str.strip()))


def _coerce_deadlines_payload(payload: Dict[str, Any]) -> List[dict]:
    deadlines = payload.get("deadlines", [])
    if not isinstance(deadlines, list):
        deadlines = []

    out: List[dict] = []
    for item in deadlines:
        if not isinstance(item, dict):
            continue
        label = item.get("label")
        date = item.get("date")
        if date is None:
            continue
        date_s = str(date).strip()
        # Keep only well-formed MM-DD-YYYY dates.
        if not _is_mm_dd_yyyy(date_s):
            continue
        out.append({"label": "" if label is None else str(label).strip().lower(), "date": date_s})
    return out


def _coerce_program_dates_payload(payload: Dict[str, Any]) -> List[dict]:
    program_dates = payload.get("program_dates", [])
    if not isinstance(program_dates, list):
        program_dates = []

    out: List[dict] = []
    for item in program_dates:
        if not isinstance(item, dict):
            continue
        label = item.get("label")
        dates = item.get("dates")
        if not isinstance(dates, list):
            continue

        # Expect exactly 2 entries; coerce if longer/shorter.
        start = ""
        end = ""
        if len(dates) >= 1 and dates[0] not in (None, ""):
            start = str(dates[0]).strip()
        if len(dates) >= 2 and dates[1] not in (None, ""):
            end = str(dates[1]).strip()

        # Validate date formatting; allow "" for missing.
        if start and not _is_mm_dd_yyyy(start):
            start = ""
        if end and not _is_mm_dd_yyyy(end):
            end = ""

        # Skip if both empty after filtering.
        if not start and not end:
            continue

        out.append(
            {
                "label": "" if label is None else str(label).strip(),
                "dates": [start, end],
            }
        )
    return out


def extract_deadlines_and_program_dates_with_gemini(
    markdown_text: str,
    program_name: str,
    cfg: GeminiConfig,
    max_chars: int,
) -> Tuple[List[dict], List[dict]]:
    md = _truncate_text(markdown_text, max_chars)
    prompt = f"""Extract the application deadline(s) AND program/session date ranges for the program named "{program_name}" from the text below.

Return ONLY valid JSON (no markdown, no code fences, no commentary) with exactly this schema:
{{
  "name": "{program_name}",
  "deadlines": [
    {{"label": "early|regular|final|priority|rolling|", "date": "MM-DD-YYYY"}}
  ],
  "program_dates": [
    {{"label": "Session 1|Session I|Summer Session|", "dates": ["MM-DD-YYYY", "MM-DD-YYYY"]}}
  ]
}}

Rules:
- Include deadlines and program dates for ANY year present in the text.
- If the page mentions applications but provides no concrete dates, return an empty deadlines array.
- If you can infer a label (early/priority/final/regular/rolling) from nearby words, include it; otherwise use "".
- Dates must be "MM-DD-YYYY" exactly. If a day is missing, do NOT guess; omit that date.
- For program/session dates: return one entry per session when possible. Put start date first and end date second.
- If only a start date is present, use ["MM-DD-YYYY", ""].
- If only an end date is present, use ["", "MM-DD-YYYY"].

Text:
{md}
"""
    raw = _gemini_generate_content(prompt, cfg, response_mime_type="application/json")
    obj = _extract_json_object(raw)
    return _coerce_deadlines_payload(obj), _coerce_program_dates_payload(obj)



def _load_results_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {"results": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            return data
    except Exception:
        pass
    return {"results": []}


def _atomic_write_json(path: str, data: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _results_index_by_id(results: List[dict]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for i, item in enumerate(results):
        if isinstance(item, dict) and isinstance(item.get("id"), str):
            idx[item["id"]] = i
    return idx


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Markdown via Jina Reader and extract application deadlines + program dates via Gemini."
    )
    parser.add_argument(
        "--input",
        default="ec_bank_rows_with_valid_url.csv",
        help="Input CSV path (default: ec_bank_rows_with_valid_url.csv).",
    )
    parser.add_argument(
        "--output",
        default="results.json",
        help="Output JSON path (default: results.json).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help='Only process the first N selected rows (default: 10; use 0 for no limit).',
    )
    parser.add_argument(
        "--rerun-existing",
        action="store_true",
        help="Reprocess only IDs already present in results.json (updates them in-place).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing even if the result already looks complete (also refetches markdown).",
    )
    parser.add_argument(
        "--only-id",
        default="",
        help='If set, only process a single row by its "id" (works with --rerun-existing).',
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Timeout per request in seconds (default: 20).",
    )
    parser.add_argument(
        "--max-links",
        type=int,
        default=1,
        help="Max additional links to crawl per program (default: 1).",
    )
    parser.add_argument(
        "--always-augment-html",
        action="store_true",
        help="Always append a full HTML->text extraction to each markdown file (more complete, larger files).",
    )
    parser.add_argument(
        "--min-md-chars",
        type=int,
        default=25000,
        help="If Jina markdown is shorter than this, append full HTML->text (default: 25000).",
    )
    parser.add_argument(
        "--fetch-retries",
        type=int,
        default=3,
        help="Retries for fetching markdown (default: 3).",
    )
    parser.add_argument(
        "--fetch-backoff",
        type=float,
        default=2.0,
        help="Backoff base seconds for fetch retries (default: 2.0).",
    )
    parser.add_argument(
        "--min-domain-interval",
        type=float,
        default=1.5,
        help="Minimum seconds between requests to the same domain (default: 1.5).",
    )
    parser.add_argument(
        "--max-markdown-chars",
        type=int,
        default=120000,
        help="Max characters of markdown to send to Gemini (default: 120000).",
    )
    parser.add_argument(
        "--max-output-tokens",
        type=int,
        default=1200,
        help="Gemini max output tokens (default: 1200).",
    )
    parser.add_argument(
        "--gemini-retries",
        type=int,
        default=2,
        help="Retries for Gemini calls (default: 2).",
    )
    parser.add_argument(
        "--gemini-backoff",
        type=float,
        default=2.0,
        help="Backoff base seconds for Gemini retries (default: 2.0).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-row progress/errors to stderr.",
    )
    parser.add_argument(
        "--no-tqdm",
        action="store_true",
        help="Disable tqdm progress bar (even if tqdm is installed).",
    )
    parser.add_argument(
        "--md-dir",
        default="md-files",
        help="Directory to store fetched markdown files (default: md-files).",
    )
    parser.add_argument(
        "--options-path",
        default=OPTIONS_PY_PATH,
        help="Path to options file (default: options.py).",
    )
    args = parser.parse_args(argv)

    # Load .env.local if present
    load_env_local(".env.local")
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        print(
            'Missing GEMINI_API_KEY. Set it in environment or in ".env.local".',
            file=sys.stderr,
        )
        return 2

    fetch_cfg = FetchConfig(
        timeout_seconds=float(args.timeout),
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        retries=max(0, int(args.fetch_retries)),
        backoff_seconds=max(0.0, float(args.fetch_backoff)),
        min_domain_interval_seconds=max(0.0, float(args.min_domain_interval)),
    )
    gem_cfg = GeminiConfig(
        api_key=api_key,
        model=MODEL,
        timeout_seconds=float(args.timeout),
        max_output_tokens=int(args.max_output_tokens),
        retries=max(0, int(args.gemini_retries)),
        backoff_seconds=max(0.0, float(args.gemini_backoff)),
    )
    jina_cfg = JinaConfig(
        timeout_seconds=float(args.timeout),
        retries=max(0, int(args.fetch_retries)),
        backoff_seconds=max(0.0, float(args.fetch_backoff)),
    )
    options_cfg = load_options_from_options_py(args.options_path)

    fieldnames, rows = _read_csv_rows(args.input)
    for required in ("url", "valid_url", "title", "id", "raw_text"):
        if required not in fieldnames:
            raise ValueError(f'CSV missing required column "{required}".')

    # Load/initialize results file and skip IDs we've already processed.
    results_data = _load_results_json(args.output)
    results_list: List[dict] = results_data.get("results", [])
    if not isinstance(results_list, list):
        results_list = []
        results_data["results"] = results_list
    existing_by_id = _results_index_by_id(results_list)

    def needs_processing(existing_item: Optional[dict]) -> bool:
        if args.force:
            return True
        # Reprocess failures so prompt/parsing improvements can fix them.
        if not isinstance(existing_item, dict):
            return True
        if existing_item.get("error"):
            return True
        # Upgrade if missing any of our extracted fields.
        for k in (
            "deadlines",
            "program_dates",
            "details",
            "details_tags",
            "mode",
            "price",
            "eligibility",
            "grade_level",
            "location",
        ):
            if k not in existing_item:
                return True
        return False

    indices: List[int] = []

    if args.rerun_existing:
        # Reprocess only the IDs already present in results.json.
        id_to_row_index: Dict[str, int] = {}
        for i, r in enumerate(rows):
            rid = str(r.get("id") or "").strip()
            if rid:
                id_to_row_index[rid] = i
        target_only = (args.only_id or "").strip()
        for rid in existing_by_id.keys():
            if target_only and rid != target_only:
                continue
            i = id_to_row_index.get(rid)
            if i is None:
                continue
            r = rows[i]
            if (r.get("valid_url") or "").strip().upper() != TRUE_STR:
                continue
            existing_item = results_list[existing_by_id[rid]]
            if not needs_processing(existing_item):
                continue
            indices.append(i)
    else:
        # Default: process URLs already validated, skipping IDs already present in results.json.
        target_only = (args.only_id or "").strip()
        for i, r in enumerate(rows):
            if (r.get("valid_url") or "").strip().upper() != TRUE_STR:
                continue
            rid = str(r.get("id") or "").strip()
            if not rid:
                continue
            if target_only and rid != target_only:
                continue
            if rid in existing_by_id:
                continue
            indices.append(i)

    if args.limit and args.limit > 0:
        indices = indices[: int(args.limit)]

    iterator: Iterable[int] = indices
    if tqdm is not None and not args.no_tqdm:
        iterator = tqdm(indices, unit="row", desc="Extracting metadata/dates")

    os.makedirs(args.md_dir, exist_ok=True)
    limiter = DomainRateLimiter(max(fetch_cfg.min_domain_interval_seconds, 0.0))

    def _pick_single(value: Any, allowed: List[str]) -> str:
        v = "" if value is None else str(value).strip()
        if not v:
            return ""
        # Case-insensitive matching
        v_low = v.lower()
        
        # Common synonym mapping
        if v_low in ("residential", "on-campus", "on campus"):
            v_low = "in-person"
        if v_low in ("online", "virtual"):
            v_low = "remote"
            
        for opt in allowed:
            if opt.lower() == v_low:
                return opt
        return ""

    def _pick_multi(value: Any, allowed: List[str]) -> List[str]:
        if not isinstance(value, list):
            return []
        allowed_map = {opt.lower(): opt for opt in allowed}
        out: List[str] = []
        for x in value:
            s = "" if x is None else str(x).strip().lower()
            if s in allowed_map:
                final_val = allowed_map[s]
                if final_val not in out:
                    out.append(final_val)
        return out

    for idx in iterator:
        row = rows[idx]
        url = (row.get("url") or "").strip()
        program_name = (row.get("title") or "").strip()
        row_id = (row.get("id") or "").strip()
        raw_text = (row.get("raw_text") or "").strip()
        if not row_id:
            continue

        # Store markdown files under md-files/<id>/ as requested.
        program_dir = os.path.join(args.md_dir, row_id)
        os.makedirs(program_dir, exist_ok=True)
        main_md_path = os.path.join(program_dir, "0_main.md")

        try:
            # ---- Fetch main markdown (or reuse / migrate) ----
            if (not args.force) and os.path.exists(main_md_path) and os.path.getsize(main_md_path) > 0:
                main_md = open(main_md_path, "r", encoding="utf-8", errors="replace").read()
            else:
                # migrate legacy single-file markdown if present: md-files/<id>__*.md
                legacy_md = ""
                try:
                    for fname in os.listdir(args.md_dir):
                        if fname.startswith(row_id + "__") and fname.endswith(".md"):
                            legacy_md_path = os.path.join(args.md_dir, fname)
                            legacy_md = open(legacy_md_path, "r", encoding="utf-8", errors="replace").read()
                            break
                except Exception:
                    legacy_md = ""

                main_md = legacy_md or fetch_markdown_full_page(
                    url,
                    jina_cfg,
                    fetch_cfg,
                    limiter,
                    always_augment_html=bool(args.always_augment_html),
                    min_md_chars_for_no_augment=int(args.min_md_chars),
                )
                with open(main_md_path, "w", encoding="utf-8") as f:
                    f.write(main_md)

            # Ensure we don't miss key date blocks due to markdown extraction gaps.
            if not _has_date_signal(main_md):
                main_md = fetch_markdown_full_page(
                    url,
                    jina_cfg,
                    fetch_cfg,
                    limiter,
                    always_augment_html=bool(args.always_augment_html),
                    min_md_chars_for_no_augment=int(args.min_md_chars),
                )
                with open(main_md_path, "w", encoding="utf-8") as f:
                    f.write(main_md)

            # ---- Round 1: choose up to N additional links to crawl ----
            # IMPORTANT: markdown often drops links; pull candidates from raw HTML anchors.
            candidates: List[Dict[str, str]] = []
            try:
                html_full_for_links = fetch_html(url, fetch_cfg, limiter)
                candidates += extract_html_links(html_full_for_links, base_url=url, limit=400)
            except Exception:
                pass

            # Also include any markdown-style links we might have captured.
            candidates += extract_markdown_links(main_md, base_url=url)

            # Dedupe candidate URLs
            dedup: Dict[str, Dict[str, str]] = {}
            for c in candidates:
                u = c.get("url", "")
                if u and u not in dedup:
                    dedup[u] = c
            candidates = list(dedup.values())

            chosen_links = choose_additional_links_with_gemini(
                program_name=program_name or row_id,
                main_md=main_md,
                base_url=url,
                candidates=candidates,
                cfg=gem_cfg,
                max_links=max(0, int(args.max_links)),
            )
            if not chosen_links:
                chosen_links = choose_links_heuristic(
                    candidates=candidates,
                    base_url=url,
                    max_links=max(0, int(args.max_links)),
                )

            # Fetch chosen pages and store their markdown in the program folder.
            sources: List[Dict[str, str]] = [{"url": url, "md_path": main_md_path}]
            for i_link, link in enumerate(chosen_links, start=1):
                link_url = link.get("url", "")
                if not link_url:
                    continue
                label = link.get("label", "") or urllib.parse.urlparse(link_url).path.strip("/").split("/")[-1]
                link_slug = _safe_slug(label) or f"link-{i_link}"
                link_md_path = os.path.join(program_dir, f"{i_link}_{link_slug}.md")
                if (not args.force) and os.path.exists(link_md_path) and os.path.getsize(link_md_path) > 0:
                    pass
                else:
                    md = fetch_markdown_full_page(
                        link_url,
                        jina_cfg,
                        fetch_cfg,
                        limiter,
                        always_augment_html=bool(args.always_augment_html),
                        min_md_chars_for_no_augment=int(args.min_md_chars),
                    )
                    with open(link_md_path, "w", encoding="utf-8") as f:
                        f.write(md)
                sources.append({"url": link_url, "md_path": link_md_path})

            # Build combined markdown across all stored pages for this program.
            combined_parts: List[str] = []
            for src in sources:
                mdp = src.get("md_path", "")
                if not mdp:
                    continue
                try:
                    txt = open(mdp, "r", encoding="utf-8", errors="replace").read()
                except Exception:
                    continue
                combined_parts.append(f"SOURCE URL: {src.get('url','')}\n\n{txt}\n\n---\n")
            combined_md = "\n".join(combined_parts)

            # ---- Unified Extraction Phase ----
            extracted = extract_all_data_unified(
                program_name=program_name or row_id,
                combined_md=combined_md,
                options=options_cfg,
                cfg=gem_cfg,
                max_chars=int(args.max_markdown_chars),
            )

            # Post-processing and rule-based augmentation
            deadlines = _coerce_deadlines_payload(extracted)
            program_dates = _coerce_program_dates_payload(extracted)
            
            # Hard guarantees: add any obvious deadlines found via deterministic parsing.
            rb_deadlines = extract_deadlines_rule_based(combined_md)
            deadlines = _dedupe_list_of_dicts(deadlines + rb_deadlines, ("label", "date"))

            # Allowed options for validation
            mode_opts = options_cfg.get("mode", {}).get("options", []) if options_cfg else []
            price_opts = options_cfg.get("price", {}).get("options", []) if options_cfg else []
            elig_opts = options_cfg.get("eligibility", {}).get("options", []) if options_cfg else []
            grade_opts = options_cfg.get("grade_level", {}).get("options", []) if options_cfg else []
            loc_opts = options_cfg.get("location", {}).get("options", []) if options_cfg else []
            det_opts = options_cfg.get("details", {}).get("options", []) if options_cfg else []

            # Deterministic metadata hints (used to patch common misses):
            inferred_elig = _infer_eligibility(combined_md + "\n" + raw_text)
            inferred_grades = _infer_grade_levels(combined_md + "\n" + raw_text)

            result_item = {
                "id": row_id,
                "name": program_name,
                "url": url,
                "deadlines": deadlines,
                "program_dates": program_dates,
                "mode": _pick_single(extracted.get("mode"), mode_opts) if isinstance(mode_opts, list) else "",
                "price": _pick_single(extracted.get("price"), price_opts) if isinstance(price_opts, list) else "",
                "eligibility": _dedupe_str_list(
                    (_pick_multi(extracted.get("eligibility"), elig_opts) if isinstance(elig_opts, list) else [])
                    + _pick_multi(inferred_elig, elig_opts),
                ),
                "grade_level": _dedupe_str_list(
                    (_pick_multi(extracted.get("grade_level"), grade_opts) if isinstance(grade_opts, list) else [])
                    + _pick_multi(inferred_grades, grade_opts),
                ),
                "location": _pick_multi(extracted.get("location"), loc_opts) if isinstance(loc_opts, list) else [],
                "details_tags": _pick_multi(extracted.get("details_tags"), det_opts) if isinstance(det_opts, list) else [],
                "details": (extracted.get("details") or "").strip() if isinstance(extracted, dict) else "",
            }

            if row_id in existing_by_id:
                results_list[existing_by_id[row_id]] = result_item
            else:
                results_list.append(result_item)
                existing_by_id[row_id] = len(results_list) - 1

            # Write incrementally so long runs are resumable.
            _atomic_write_json(args.output, results_data)

            if args.verbose:
                print(
                    f"[ok] {program_name} -> deadlines={len(deadlines)}, program_dates={len(program_dates)}, sources={len(sources)}",
                    file=sys.stderr,
                )

        except (urllib.error.URLError, urllib.error.HTTPError, socket.timeout, TimeoutError) as e:
            result_item = {
                "id": row_id,
                "name": program_name,
                "url": url,
                "deadlines": [],
                "program_dates": [],
                "details": "",
                "details_tags": [],
                "mode": "",
                "price": "",
                "eligibility": [],
                "grade_level": [],
                "location": [],
                "error": str(e),
            }
            if row_id in existing_by_id:
                results_list[existing_by_id[row_id]] = result_item
            else:
                results_list.append(result_item)
                existing_by_id[row_id] = len(results_list) - 1
            _atomic_write_json(args.output, results_data)
            if args.verbose:
                print(f"[fetch_error] {program_name}: {e}", file=sys.stderr)
        except Exception as e:
            result_item = {
                "id": row_id,
                "name": program_name,
                "url": url,
                "deadlines": [],
                "program_dates": [],
                "details": "",
                "details_tags": [],
                "mode": "",
                "price": "",
                "eligibility": [],
                "grade_level": [],
                "location": [],
                "error": str(e),
            }
            if row_id in existing_by_id:
                results_list[existing_by_id[row_id]] = result_item
            else:
                results_list.append(result_item)
                existing_by_id[row_id] = len(results_list) - 1
            _atomic_write_json(args.output, results_data)
            if args.verbose:
                print(f"[error] {program_name}: {e}", file=sys.stderr)

        time.sleep(0.05)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


