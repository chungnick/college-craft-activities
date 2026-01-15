#!/usr/bin/env python3
"""
Validate URLs in a CSV.

Reads a CSV with a `url` column and writes `TRUE`/`FALSE` into a `valid_url` column
based on whether the URL appears reachable/valid via HTTP(S).

Default behavior writes to a new output file (does not overwrite input unless --inplace).
"""

from __future__ import annotations

import argparse
import csv
import os
import socket
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from tqdm import tqdm  # type: ignore
except Exception:  # tqdm is optional
    tqdm = None  # type: ignore


TRUE_STR = "TRUE"
FALSE_STR = "FALSE"


@dataclass(frozen=True)
class ValidationConfig:
    timeout_seconds: float
    retries: int
    retry_backoff_seconds: float
    user_agent: str
    allow_insecure_ssl: bool


def _normalize_url(raw: str) -> Optional[str]:
    if raw is None:
        return None
    s = raw.strip()
    if not s:
        return None

    # If it's missing a scheme, assume https:// for common cases like "www.example.com".
    parsed = urllib.parse.urlparse(s)
    if not parsed.scheme:
        s = "https://" + s
        parsed = urllib.parse.urlparse(s)

    if parsed.scheme not in ("http", "https"):
        return None

    # Basic sanity: needs a netloc/host.
    if not parsed.netloc:
        return None

    return s


def _is_status_valid(status: int) -> bool:
    # Treat redirects (3xx) as valid because the URL resolves.
    if 200 <= status <= 399:
        return True

    # These often indicate "exists but access is restricted" or "method not allowed".
    if status in (401, 403, 405, 429):
        return True

    # 404/410 etc. are invalid, 5xx treated as invalid (server error / unreliable).
    return False


def _make_request(
    method: str,
    url: str,
    cfg: ValidationConfig,
) -> urllib.request.Request:
    headers = {
        "User-Agent": cfg.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.7",
        "Connection": "close",
    }

    # For GET, try to avoid downloading large bodies.
    if method.upper() == "GET":
        headers["Range"] = "bytes=0-0"

    return urllib.request.Request(url=url, method=method.upper(), headers=headers)


def validate_url(url: str, cfg: ValidationConfig) -> bool:
    """
    Returns True if the URL looks reachable/valid, else False.
    """
    normalized = _normalize_url(url)
    if not normalized:
        return False

    last_error: Optional[BaseException] = None
    for attempt in range(cfg.retries + 1):
        if attempt > 0:
            time.sleep(cfg.retry_backoff_seconds * attempt)

        try:
            # First try HEAD (cheap) then fallback to GET.
            for method in ("HEAD", "GET"):
                req = _make_request(method, normalized, cfg)
                with urllib.request.urlopen(req, timeout=cfg.timeout_seconds) as resp:
                    status = getattr(resp, "status", None) or resp.getcode()
                    if status is None:
                        return False
                    return _is_status_valid(int(status))

        except urllib.error.HTTPError as e:
            # HTTPError is also a response (status is meaningful).
            status = getattr(e, "code", None)
            if status is None:
                last_error = e
                continue
            return _is_status_valid(int(status))
        except (urllib.error.URLError, socket.timeout, TimeoutError) as e:
            last_error = e
            continue
        except Exception as e:  # keep script resilient across odd urllib behaviors
            last_error = e
            continue

    _ = last_error
    return False


def _read_csv_rows(path: str) -> Tuple[List[str], List[dict]]:
    # newline="" is critical for correct parsing of quoted multi-line fields.
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


def _atomic_write_csv(path: str, fieldnames: List[str], rows: List[dict]) -> None:
    """
    Write a complete CSV file and atomically replace the destination.
    This keeps the output always in a valid, readable state.
    """
    dest_dir = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".valid_url_tmp_", suffix=".csv", dir=dest_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Visit URLs in a CSV and tag TRUE/FALSE in the valid_url column."
    )
    parser.add_argument(
        "--input",
        default="ec_bank_rows.csv",
        help="Path to input CSV (default: ec_bank_rows.csv).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to output CSV (default: <input basename>_with_valid_url.csv).",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Overwrite the input file in place (ignores --output).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Timeout per request in seconds (default: 10).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel URL checkers (default: 10).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Number of retries for transient failures (default: 1).",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=1.0,
        help="Backoff seconds multiplier between retries (default: 1.0).",
    )
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (compatible; college-craft-url-validator/1.0)",
        help="User-Agent header to send (default: a generic UA).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress to stderr.",
    )
    parser.add_argument(
        "--no-tqdm",
        action="store_true",
        help="Disable tqdm progress bar (even if tqdm is installed).",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=25,
        help="Write progress to disk every N completed URL checks (default: 25).",
    )
    parser.add_argument(
        "--checkpoint-seconds",
        type=float,
        default=2.0,
        help="Also write progress to disk at most every N seconds (default: 2.0).",
    )
    args = parser.parse_args(argv)

    input_path = args.input
    if args.inplace:
        output_path = input_path
    else:
        if args.output:
            output_path = args.output
        else:
            if input_path.lower().endswith(".csv"):
                output_path = input_path[:-4] + "_with_valid_url.csv"
            else:
                output_path = input_path + "_with_valid_url.csv"

    cfg = ValidationConfig(
        timeout_seconds=args.timeout,
        retries=max(0, args.retries),
        retry_backoff_seconds=max(0.0, args.retry_backoff),
        user_agent=args.user_agent,
        allow_insecure_ssl=False,
    )

    fieldnames, rows = _read_csv_rows(input_path)

    if "url" not in fieldnames:
        raise ValueError('CSV missing required column "url".')

    if "valid_url" not in fieldnames:
        # Append to end to preserve existing order.
        fieldnames = fieldnames + ["valid_url"]

    total = len(rows)

    # Map normalized URL -> list of row indices using it. Validate each normalized URL once.
    url_to_row_idxs: Dict[str, List[int]] = {}
    invalid_row_idxs: List[int] = []

    for idx, row in enumerate(rows):
        raw_url = (row.get("url") or "").strip()
        normalized = _normalize_url(raw_url)
        if not normalized:
            invalid_row_idxs.append(idx)
            continue
        url_to_row_idxs.setdefault(normalized, []).append(idx)

    # Mark obviously invalid URLs immediately.
    for idx in invalid_row_idxs:
        rows[idx]["valid_url"] = FALSE_STR

    unique_urls = list(url_to_row_idxs.keys())

    pbar = None
    if tqdm is not None and not args.no_tqdm:
        pbar = tqdm(total=total, unit="row", desc="Validating URLs")
        if invalid_row_idxs:
            pbar.update(len(invalid_row_idxs))

    def log(msg: str) -> None:
        if not args.verbose:
            return
        if tqdm is not None and not args.no_tqdm:
            try:
                tqdm.write(msg, file=sys.stderr)  # type: ignore[attr-defined]
                return
            except Exception:
                pass
        print(msg, file=sys.stderr)

    # Periodic checkpointing to update the CSV on disk as we go.
    completed_checks = 0
    last_checkpoint_t = 0.0

    def maybe_checkpoint(force: bool = False) -> None:
        nonlocal completed_checks, last_checkpoint_t
        if args.checkpoint_every <= 0 and args.checkpoint_seconds <= 0:
            return
        now = time.time()
        due_by_count = args.checkpoint_every > 0 and completed_checks % args.checkpoint_every == 0
        due_by_time = args.checkpoint_seconds > 0 and (now - last_checkpoint_t) >= args.checkpoint_seconds
        if force or due_by_count or due_by_time:
            _atomic_write_csv(output_path, fieldnames, rows)
            last_checkpoint_t = now

    workers = max(1, int(args.workers))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {
            executor.submit(validate_url, url, cfg): url
            for url in unique_urls
        }

        for fut in as_completed(future_to_url):
            url = future_to_url[fut]
            try:
                is_valid = bool(fut.result())
            except Exception:
                is_valid = False

            idxs = url_to_row_idxs.get(url, [])
            val = TRUE_STR if is_valid else FALSE_STR
            for idx in idxs:
                rows[idx]["valid_url"] = val

            completed_checks += 1
            if pbar is not None:
                pbar.update(len(idxs))

            if args.verbose:
                # Only log one representative row/url to keep output readable.
                log(f"{url} -> {val} ({len(idxs)} row(s))")

            maybe_checkpoint(force=False)

    if pbar is not None:
        pbar.close()

    # Final write (always).
    _atomic_write_csv(output_path, fieldnames, rows)

    if args.verbose:
        print(f"Wrote: {output_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


