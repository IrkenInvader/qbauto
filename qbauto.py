#!/usr/bin/env python3
"""qbauto.py - watches a folder for .txt query files, searches qBittorrent
via its Web API, picks the max-seed result, and adds it. Saves into a
subfolder of SAVE_ROOT that mirrors the query file's top-level folder name."""

import base64
import logging
import os
import shutil
import sys
import threading
import time
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

QB_HOST = "127.0.0.1"
QB_PORT = 8080
QB_USER = "admin"
QB_PASSWORD = "adminadmin"

WATCHED_ROOT = r"C:\Users\lukev\qbauto\watched"
SAVE_ROOT = r"C:\Users\lukev\qbauto\downloads"

POLL_INTERVAL = 5
SEARCH_TIMEOUT = 15
MAX_CONCURRENT_SEARCHES = 5
STABLE_FILE_AGE = 2
LOG_FILE = str(Path(__file__).resolve().parent / "qbauto.log")

SKIP_DIRS = {"done", "error", "skipped"}

logger = logging.getLogger("qbauto")


def setup_logging():
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%S")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stderr)
    sh.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(fh)
    root.addHandler(sh)


class QBClient:
    def __init__(self, host, port, user, password):
        self.base = f"http://{host}:{port}"
        self.user = user
        self.password = password
        self.session = requests.Session()
        self.lock = threading.RLock()

    def _login_locked(self):
        data = {"username": self.user, "password": self.password}
        resp = self.session.post(self.base + "/api/v2/auth/login", data=data, timeout=30)
        if resp.text.strip() != "Ok.":
            raise RuntimeError(f"qBittorrent login failed: {resp.text!r}")
        logger.info("Logged in to qBittorrent")

    def login(self):
        with self.lock:
            self._login_locked()

    def _request(self, method, path, **kwargs):
        with self.lock:
            url = self.base + path
            resp = self.session.request(method, url, timeout=30, **kwargs)
            if resp.status_code == 403:
                logger.warning("Got 403, re-logging in")
                self._login_locked()
                resp = self.session.request(method, url, timeout=30, **kwargs)
            resp.raise_for_status()
            return resp

    def search_start(self, pattern):
        data = {"pattern": pattern, "category": "all", "plugins": "all"}
        body = self._request("POST", "/api/v2/search/start", data=data).json()
        if "id" not in body:
            raise RuntimeError(f"search/start did not return an id: {body!r}")
        return int(body["id"])

    def search_results(self, search_id, limit=1000, offset=0):
        params = {"id": search_id, "limit": limit, "offset": offset}
        return self._request("GET", "/api/v2/search/results", params=params).json()

    def search_all_results(self, search_id):
        body = self.search_results(search_id)
        results = list(body.get("results", []) or [])
        total = body.get("total", len(results))
        offset = len(results)
        while offset < total:
            body = self.search_results(search_id, offset=offset)
            chunk = body.get("results", []) or []
            if not chunk:
                break
            results.extend(chunk)
            offset += len(chunk)
        return results, body.get("status", "Running")

    def search_stop(self, search_id):
        try:
            self._request("POST", "/api/v2/search/stop", data={"id": search_id})
        except Exception as e:
            logger.debug(f"search/stop {search_id} failed: {e}")

    def search_delete(self, search_id):
        try:
            self._request("POST", "/api/v2/search/delete", data={"id": search_id})
        except Exception as e:
            logger.debug(f"search/delete {search_id} failed: {e}")

    def torrents_info_by_hash(self, info_hash):
        return self._request("GET", "/api/v2/torrents/info", params={"hashes": info_hash}).json()

    def torrents_info_all(self):
        return self._request("GET", "/api/v2/torrents/info").json()

    def torrents_add(self, urls, savepath):
        data = {"urls": urls, "savepath": savepath}
        text = self._request("POST", "/api/v2/torrents/add", data=data).text.strip()
        if text.lower().startswith("fail"):
            raise RuntimeError(f"torrents/add failed: {text!r}")


def parse_magnet_hash(file_url):
    if not file_url.startswith("magnet:"):
        return None
    parsed = urllib.parse.urlparse(file_url)
    xt = urllib.parse.parse_qs(parsed.query).get("xt", [None])[0]
    if not xt:
        return None
    parts = xt.split(":")
    if len(parts) < 3 or parts[0] != "urn" or parts[1] != "btih":
        return None
    raw = parts[2].lower()
    if len(raw) == 40:
        return raw
    if len(raw) == 32:
        try:
            return base64.b32decode(raw.upper()).hex()
        except Exception:
            return raw
    return raw


def is_duplicate(client, winner):
    file_url = winner.get("fileUrl", "")
    name = winner.get("fileName", "")
    h = parse_magnet_hash(file_url)
    if h:
        return bool(client.torrents_info_by_hash(h))
    return any(t.get("name", "").lower() == name.lower() for t in client.torrents_info_all())


def process_query(client, query, save_subfolder):
    savepath = str(Path(SAVE_ROOT) / save_subfolder) if save_subfolder else str(Path(SAVE_ROOT))
    sid = client.search_start(query)
    logger.info(f"[search] started id={sid} query={query!r}")
    best = None
    deadline = time.time() + SEARCH_TIMEOUT
    try:
        while time.time() < deadline:
            results, status = client.search_all_results(sid)
            for r in results:
                seeds = int(r.get("nbSeeders", 0))
                if best is None or seeds > int(best.get("nbSeeders", 0)):
                    best = r
            if status == "Stopped":
                break
            time.sleep(1)
    finally:
        client.search_stop(sid)
        client.search_delete(sid)

    if best is None:
        logger.warning(f"[search] no results for {query!r}")
        return ("error", query, None)

    seeds = int(best.get("nbSeeders", 0))
    title = best.get("fileName", "?")
    file_url = best.get("fileUrl", "")
    logger.info(f"[search] winner query={query!r} title={title!r} seeds={seeds}")

    if is_duplicate(client, best):
        logger.info(f"[dup] already in qB: {title!r}")
        return ("skipped", query, title)

    os.makedirs(savepath, exist_ok=True)
    client.torrents_add(file_url, savepath)
    logger.info(f"[add] {title!r} -> {savepath}")
    return ("added", query, title)


def move_to_folder(src_file, dest_dirname):
    src = Path(src_file)
    dest_dir = src.parent / dest_dirname
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    if dest.exists():
        stem, suffix = dest.stem, dest.suffix
        i = 1
        while dest.exists():
            dest = dest_dir / f"{stem}.{i}{suffix}"
            i += 1
    shutil.move(str(src), str(dest))
    return dest


def outcome_summary(outcomes):
    c = Counter(o[0] for o in outcomes)
    return ", ".join(f"{k}={v}" for k, v in sorted(c.items()))


def process_file(client, file_path):
    rel = Path(file_path).relative_to(WATCHED_ROOT)
    parts = rel.parts
    if len(parts) < 2:
        logger.warning(f"[skip] {file_path} is directly in watched root (no subfolder); ignoring")
        return
    save_subfolder = parts[0]
    if save_subfolder.lower() in SKIP_DIRS:
        return

    try:
        text = Path(file_path).read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        logger.error(f"[file] could not read {file_path}: {e}")
        return

    seen = set()
    queries = []
    for ln in (s.strip() for s in text.splitlines()):
        if ln and ln not in seen:
            seen.add(ln)
            queries.append(ln)

    if not queries:
        logger.warning(f"[file] {file_path} has no queries; leaving in place")
        return

    logger.info(f"[file] processing {file_path} ({len(queries)} queries) -> save/{save_subfolder}")
    outcomes = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SEARCHES) as pool:
        futs = {pool.submit(process_query, client, q, save_subfolder): q for q in queries}
        for fut in as_completed(futs):
            q = futs[fut]
            try:
                outcomes.append(fut.result())
            except Exception as e:
                logger.error(f"[query] {q!r} crashed: {e}", exc_info=True)
                outcomes.append(("error", q, None))

    if any(o[0] == "error" for o in outcomes):
        dest = "error"
    elif any(o[0] == "skipped" for o in outcomes):
        dest = "skipped"
    else:
        dest = "done"

    move_to_folder(file_path, dest)
    logger.info(f"[file] {file_path} -> {dest}/ ({outcome_summary(outcomes)})")


def find_query_files():
    found = []
    now = time.time()
    for root, dirs, files in os.walk(WATCHED_ROOT):
        dirs[:] = [d for d in dirs if d.lower() not in SKIP_DIRS]
        for f in files:
            if not f.lower().endswith(".txt"):
                continue
            fp = os.path.join(root, f)
            try:
                if now - os.path.getmtime(fp) >= STABLE_FILE_AGE:
                    found.append(fp)
            except OSError:
                continue
    return found


def main():
    setup_logging()
    os.makedirs(WATCHED_ROOT, exist_ok=True)
    os.makedirs(SAVE_ROOT, exist_ok=True)
    client = QBClient(QB_HOST, QB_PORT, QB_USER, QB_PASSWORD)
    try:
        client.login()
    except Exception as e:
        logger.error(f"Initial qBittorrent login failed: {e}")
        logger.error("Ensure qBittorrent is running and Web UI is enabled with matching credentials.")
        sys.exit(1)
    logger.info(f"Daemon started. Watching {WATCHED_ROOT}; saving to {SAVE_ROOT}")
    try:
        while True:
            try:
                for fp in find_query_files():
                    process_file(client, fp)
            except Exception as e:
                logger.error(f"[scan] error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutdown requested; exiting")


if __name__ == "__main__":
    main()
