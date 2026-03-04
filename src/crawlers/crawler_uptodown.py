import re
import time
from collections.abc import Iterator
from queue import Empty, Queue
from threading import Lock, Thread
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright
from config import get_config
from src.crawlers.base import BaseCrawler, resolve_category_key
from src.crawlers.common import (
    BROWSER_ARGS,
    USER_AGENT,
    TIMEOUT_LOAD_STATE_MS,
    TIMEOUT_NAVIGATION_MS,
    TIMEOUT_SELECTOR_MS,
)
from src.exception import CrawlerDownloadError, CrawlerPageError
from src.utils import hash_file

# uptodown support multiple languages, so it is necessary to specify the language.
BASE = "https://en.uptodown.com/android/"
# category can append in url,like we selected "RPG", the url will be "https://en.uptodown.com/android/rpg".
# if category is "Kids", the url will be "https://en.uptodown.com/android/kids".
GAME_CATEGORIES = {
    "RPG": "rpg",
    "Strategy": "strategy",
    "Casual": "casual",
    "Emulator": "emulators",
    "Arcade": "arcade",
    "Puzzle": "puzzle",
    "Sports": "sports",
    "Racing/Sim": "racing-sims",
    "Action/Adventure": "action-adventure",
    "Other": "other-games",
    "Platform": "platformer",
    "Kids": "kids",
    "Card": "card-games",
    "NewReleases": "newreleases", # the latest released games.
    "TopDownloads": "top", # the most downloaded games
}


# Uptodown download page, download button needs a short wait before click.
DOWNLOAD_BTN_SELECTORS = [
    "button#detail-download-button",
    "button.download:has(strong:has-text('Download'))",
    "button.download",
    "a[data-download]",
    "a.download[href]",
    "a:has-text('Download')",
]
DOWNLOAD_BTN_WAIT_SEC = 5

def _is_app_game_url(href: str) -> bool:
    if not href or "uptodown.com/android" not in href:
        return False
    try:
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").rstrip("/") or "/"
        if host == "en.uptodown.com":
            return False
        if not host.endswith(".en.uptodown.com"):
            return False
        if path != "/android":
            return False
        return True
    except Exception:
        return False


def _version_ge(stored: str, list_ver: str) -> bool:
    def parse(s: str) -> tuple[int, ...]:
        out = []
        for part in (s or "").strip().split("."):
            try:
                out.append(int(part))
            except ValueError:
                out.append(0)
        return tuple(out) if out else (0,)
    p1, p2 = parse(stored), parse(list_ver)
    n = max(len(p1), len(p2))
    p1 = p1 + (0,) * (n - len(p1))
    p2 = p2 + (0,) * (n - len(p2))
    return p1 >= p2


def _slug_from_game_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        if host.endswith(".en.uptodown.com"):
            return host.replace(".en.uptodown.com", "")
        return None
    except Exception:
        return None


def _collect_links_from_container(page, container_selector: str, seen: set[str], items: list[dict]) -> None:
    container = page.locator(container_selector)
    if container.count() == 0:
        return
    for a in container.locator('a[href*=".en.uptodown.com/android"]').all():
        try:
            href = (a.get_attribute("href") or "").strip()
            if not _is_app_game_url(href) or href in seen:
                continue
            seen.add(href)
            slug = _slug_from_game_url(href)
            if not slug:
                continue
            items.append({"app_id": slug, "version": "", "slug": slug, "game_url": href})
        except Exception:
            continue


def _parse_category_page_items(page, is_first_page: bool = False) -> list[dict]:
    seen: set[str] = set()
    items: list[dict] = []
    if is_first_page:
        _collect_links_from_container(page, "#list-top-items", seen, items)
    _collect_links_from_container(page, "#content-list", seen, items)
    return items


def _get_latest_version_id_and_text(page) -> tuple[str | None, str]:
    el = page.locator("[data-version-id]").first
    if el.count() == 0:
        return None, ""
    vid = el.get_attribute("data-version-id")
    version_text = ""
    try:
        parent = el.locator("xpath=..")
        if parent.count():
            version_text = parent.inner_text().strip() or ""
        if not version_text and el.count():
            version_text = el.inner_text().strip() or ""
    except Exception:
        pass
    if version_text:
        m = re.search(r"(\d+\.\d+(?:\.\d+)*)", version_text)
        if m:
            version_text = m.group(1)
    return vid, version_text


def _get_version_from_download_page(page) -> str:
    for sel in ["[class*='version']", "h1", ".app-name", "[data-download-version]"]:
        el = page.locator(sel).first
        if el.count() == 0:
            continue
        try:
            text = el.inner_text() or el.get_attribute("data-download-version") or ""
            m = re.search(r"(\d+\.\d+(?:\.\d+)*)", text)
            if m:
                return m.group(1)
        except Exception:
            continue
    return ""


def _find_download_button(page):
    for sel in DOWNLOAD_BTN_SELECTORS:
        btn = page.locator(sel).first
        if btn.count() == 0:
            continue
        try:
            if not btn.is_visible():
                continue
            tag = btn.evaluate("el => el.tagName").lower() if btn.count() else ""
            href = btn.get_attribute("href")
            if tag == "button":
                return btn
            if href and ("/android/download" in href or "uptodown.com" in href or href.startswith("#")):
                pass
            elif href and (href.endswith(".apk") or "d.uptodown" in href or "download" in href.lower()):
                return btn
            if "download" in (btn.get_attribute("class") or "").lower() or "download" in (btn.inner_text() or "").lower():
                return btn
        except Exception:
            continue
    return None

# True if the main download button is for the Uptodown client.
def _is_uptodown_client_download_page(page) -> bool:
    try:
        # p.size under the main button contains "UPTODOWN app store" when button is client
        size_el = page.locator("p.size").first
        if size_el.count() and size_el.is_visible():
            text = size_el.inner_text() or ""
            if "UPTODOWN app store" in text:
                return True
        # or icon-logo-white-store.svg near the download area
        icon = page.locator('img[src*="icon-logo-white-store.svg"]').first
        if icon.count() and icon.is_visible():
            return True
    except Exception:
        pass
    return False

# when current page is 'Uptodown client' download, redirect to the direct file download page.
def _ensure_direct_download_page(page, download_page_url: str) -> bool:
    current = (page.url or "").rstrip("/")
    # already on -x page, nothing to do
    if re.search(r"/download/\d+-x$", current):
        return False
    # URL has version id: .../download/{vid} -> .../download/{vid}-x
    m = re.search(r"(.*/download)/(\d+)$", current)
    if m:
        base, vid = m.group(1), m.group(2)
        page.goto(f"{base}/{vid}-x", wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
        page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
        return True
    # URL is .../download (no version id): open All variants panel and get first variant -x link
    if not current.endswith("/download"):
        return False
    all_variants = page.locator('a:has-text("All variants"), button:has-text("All variants")').first
    if all_variants.count() == 0:
        return False
    all_variants.click()
    time.sleep(0.5)
    page.wait_for_selector("#contentMenuPanel section.variants .variant", state="visible", timeout=TIMEOUT_SELECTOR_MS)
    # get first variant's onclick -> location.href='.../download/123-x'
    variant = page.locator("#contentMenuPanel section.variants .variant").first
    onclick = variant.get_attribute("onclick") or ""
    for child in variant.locator("[onclick]").all():
        o = child.get_attribute("onclick") or ""
        if "location.href" in o and "/download/" in o and "-x" in o:
            onclick = o
            break
    match = re.search(r"location\.href\s*=\s*['\"]([^'\"]+/download/\d+-x)['\"]", onclick)
    if not match:
        return False
    direct_url = match.group(1)
    page.goto(direct_url, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
    page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
    return True


class UptodownCrawler(BaseCrawler):
    def __init__(self, storage, logger):
        super().__init__("uptodown", storage, logger)

    def resolve_download_url(
        self, app_id: str, version: str, game_url: str | None = None, **kwargs: object
    ) -> tuple[str, dict[str, str]]:
        url = game_url or kwargs.get("game_url")
        if not url:
            url = f"https://{app_id}.en.uptodown.com/android"
        url = url.rstrip("/")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=get_config().browser.headless, args=BROWSER_ARGS)
            try:
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                )
                page = context.new_page()
                page.goto(f"{url}/versions", wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                time.sleep(1.5)
                vid, _ = _get_latest_version_id_and_text(page)
                if not vid:
                    raise CrawlerPageError("no data-version-id on versions page", source_name=self.source_name, app_id=app_id)
                download_page_url = f"{url}/download/{vid}"
                page.goto(download_page_url, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                page.wait_for_selector("button#detail-download-button", state="visible", timeout=TIMEOUT_SELECTOR_MS)
                time.sleep(DOWNLOAD_BTN_WAIT_SEC)
                btn = _find_download_button(page)
                if not btn:
                    raise CrawlerPageError("no download button on download page", source_name=self.source_name, app_id=app_id)
                if _is_uptodown_client_download_page(page):
                    if _ensure_direct_download_page(page, download_page_url):
                        page.wait_for_selector("button#detail-download-button", state="visible", timeout=TIMEOUT_SELECTOR_MS)
                        time.sleep(DOWNLOAD_BTN_WAIT_SEC)
                        btn = _find_download_button(page)
                        if not btn:
                            raise CrawlerPageError("no download button after redirect to direct page", source_name=self.source_name, app_id=app_id)
                        download_page_url = page.url
                dl_href = btn.get_attribute("href")
                if dl_href and (dl_href.endswith(".apk") or "d.uptodown" in dl_href):
                    if dl_href.startswith("//"):
                        dl_href = "https:" + dl_href
                    elif dl_href.startswith("/"):
                        dl_href = urljoin(download_page_url, dl_href)
                    page.goto(dl_href, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                    final_url = page.url
                else:
                    final_url = download_page_url
                cookies = context.cookies()
                cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
                ua = page.evaluate("() => navigator.userAgent")
                return (final_url, {"Cookie": cookie_str, "User-Agent": ua})
            finally:
                browser.close()

    def fetch(self, app_id: str, version: str, **kwargs: object) -> str | None:
        game_url = kwargs.get("game_url")
        if not game_url:
            game_url = f"https://{app_id}.en.uptodown.com/android"
        game_url = game_url.rstrip("/")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=get_config().browser.headless, args=BROWSER_ARGS)
            try:
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                )
                page = context.new_page()
                download_page_url = f"{game_url}/download"
                page.goto(download_page_url, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                page.wait_for_selector("button#detail-download-button", state="visible", timeout=TIMEOUT_SELECTOR_MS)
                time.sleep(DOWNLOAD_BTN_WAIT_SEC)
                btn = _find_download_button(page)
                if not btn:
                    page.goto(f"{game_url}/versions", wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                    page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                    time.sleep(1.5)
                    vid, version_text = _get_latest_version_id_and_text(page)
                    if not vid:
                        raise CrawlerPageError(
                            "no data-version-id on versions page and no download button on /download",
                            source_name=self.source_name,
                            app_id=app_id,
                        )
                    download_page_url = f"{game_url}/download/{vid}"
                    page.goto(download_page_url, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                    page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                    page.wait_for_selector("button#detail-download-button", state="visible", timeout=TIMEOUT_SELECTOR_MS)
                    time.sleep(DOWNLOAD_BTN_WAIT_SEC)
                    btn = _find_download_button(page)
                    resolved_version = version_text or version or "latest"
                else:
                    resolved_version = _get_version_from_download_page(page) or version or "latest"
                if btn and _is_uptodown_client_download_page(page):
                    if _ensure_direct_download_page(page, download_page_url):
                        page.wait_for_selector("button#detail-download-button", state="visible", timeout=TIMEOUT_SELECTOR_MS)
                        time.sleep(DOWNLOAD_BTN_WAIT_SEC)
                        btn = _find_download_button(page)
                        download_page_url = page.url
                        resolved_version = _get_version_from_download_page(page) or resolved_version or version or "latest"
                if not btn:
                    raise CrawlerPageError("no download button on download page", source_name=self.source_name, app_id=app_id)
                index = self._storage.read_index()
                index_max: dict[str, str] = {}
                for e in index:
                    aid, ver = e.get("app_id"), (e.get("version") or "").strip()
                    if not aid or not ver:
                        continue
                    if aid not in index_max or _version_ge(ver, index_max[aid]):
                        index_max[aid] = ver
                stored = index_max.get(app_id)
                if stored is not None and _version_ge(stored, resolved_version):
                    self._logger.warning(f'skip game="{app_id}" reason="Latest version already installed"')
                    return None
                filename = self._apk_filename(app_id, resolved_version)
                dest_path = self._storage.apk_path(filename)
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                timeout_ms = int((get_config().download.timeout_seconds or 300) * 1000)
                btn.scroll_into_view_if_needed()
                time.sleep(0.5)
                with page.expect_download(timeout=timeout_ms) as download_ctx:
                    btn.click(no_wait_after=True)
                download = download_ctx.value
                download.save_as(dest_path)
                if download.failure():
                    raise CrawlerDownloadError(str(download.failure()), source_name=self.source_name, app_id=app_id)
                md5_hex = hash_file(dest_path)
                self._storage.append_entry({
                    "app_id": app_id,
                    "version": resolved_version,
                    "source": self.source_name,
                    "path": filename,
                    "hash": md5_hex,
                })
                self._logger.info(f"{self.source_name} staged {app_id} {resolved_version}")
                return resolved_version
            finally:
                browser.close()

    def discover_latest(self, category: str) -> list[dict]:
        return get_full_game_list_for_category(category)

    def get_full_game_list(self, category: str) -> list[dict]:
        return get_full_game_list_for_category(category, include_versions=True)

    def get_category_game_list(self, category: str) -> list[dict]:
        return get_full_game_list_for_category(category, include_versions=False)

    def iter_category_pages_with_versions(self, category: str) -> Iterator[tuple[list[dict], bool]]:
        resolved = resolve_category_key(category, GAME_CATEGORIES)
        if resolved in GAME_CATEGORIES:
            category_slug = GAME_CATEGORIES[resolved]
        else:
            category_slug = resolved.strip().lower().replace(" ", "-")
        base_url = BASE.rstrip("/") + "/" + category_slug
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=get_config().browser.headless, args=BROWSER_ARGS)
            try:
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                )
                page = context.new_page()
                pagenum = 1
                while True:
                    page_url = f"{base_url}/{pagenum}" if pagenum > 1 else base_url
                    try:
                        page.goto(page_url, wait_until="load", timeout=TIMEOUT_NAVIGATION_MS)
                        time.sleep(2)
                    except Exception:
                        yield ([], True)
                        return
                    if _is_category_no_more_pages(page, pagenum):
                        yield ([], True)
                        return
                    items = _parse_category_page_items(page, is_first_page=(pagenum == 1))
                    if items:
                        _fetch_versions_for_items(items)
                    yield (items, False)
                    pagenum += 1
            finally:
                browser.close()

def _fetch_versions_worker(
    queue: Queue,
    results: list[str],
    headless: bool,
) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=BROWSER_ARGS)
        try:
            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            page = context.new_page()
            while True:
                try:
                    idx, game_url = queue.get_nowait()
                except Empty:
                    break
                try:
                    page.goto(f"{game_url.rstrip('/')}/versions", wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                    time.sleep(1.5)
                    _, version_text = _get_latest_version_id_and_text(page)
                    results[idx] = version_text or "latest"
                except Exception:
                    results[idx] = "latest"
        finally:
            browser.close()


def _fetch_versions_for_items(items: list[dict]) -> None:
    n = len(items)
    if n == 0:
        return
    results: list[str] = [""] * n
    q: Queue = Queue()
    for i, it in enumerate(items):
        if it.get("game_url"):
            q.put((i, it["game_url"]))
    max_workers = get_config().thread_pool.max_workers
    workers_count = min(max_workers, n)
    workers_count = max(1, workers_count)
    headless = get_config().browser.headless
    threads = [
        Thread(target=_fetch_versions_worker, args=(q, results, headless))
        for _ in range(workers_count)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for i in range(n):
        if results[i]:
            items[i]["version"] = results[i]
        elif items[i].get("game_url"):
            items[i]["version"] = "latest"


def _page_number_from_url(url: str) -> int:
    try:
        path = (urlparse(url).path or "").rstrip("/")
        if not path:
            return 1
        last = path.split("/")[-1]
        return int(last) if last.isdigit() else 1
    except Exception:
        return 1


def _is_not_found_page(page) -> bool:
    try:
        loc = page.locator(".not-found")
        if loc.count() > 0 and loc.first.is_visible():
            return True
        return False
    except Exception:
        return False

# 2 situations: 404 or redirect back to a previous page . both means no more pages.
def _is_category_no_more_pages(page, requested_pagenum: int) -> bool:
    if _is_not_found_page(page):
        return True
    if requested_pagenum > 1:
        actual_page = _page_number_from_url(page.url)
        if actual_page < requested_pagenum:
            return True
    return False


def _peek_page_worker(
    base_url: str,
    next_page_list: list[int],
    claimed: set[int],
    merged_pages: set[int],
    all_items: list[dict],
    seen_urls: set[str],
    max_detected: list[bool],
    max_page_value: list[int],
    lock: Lock,
    headless: bool,
) -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=BROWSER_ARGS)
        try:
            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            page = context.new_page()
            while True:
                with lock:
                    if max_detected[0] and next_page_list[0] > max_page_value[0]:
                        return
                    n = next_page_list[0]
                    next_page_list[0] += 1
                    claimed.add(n)
                    if max_detected[0] and n > max_page_value[0]:
                        return
                page_url = f"{base_url}/{n}" if n > 1 else base_url
                try:
                    page.goto(page_url, wait_until="load", timeout=TIMEOUT_NAVIGATION_MS)
                    time.sleep(2)
                except Exception:
                    with lock:
                        max_detected[0] = True
                        max_page_value[0] = max(1, n - 1)
                    continue
                if _is_not_found_page(page):
                    with lock:
                        max_detected[0] = True
                        max_page_value[0] = max(1, n - 1)
                    continue
                actual_page = _page_number_from_url(page.url)
                items = _parse_category_page_items(page, is_first_page=(actual_page == 1))
                with lock:
                    if actual_page not in merged_pages:
                        merged_pages.add(actual_page)
                        for it in items:
                            url = it.get("game_url") or ""
                            if url and url not in seen_urls:
                                seen_urls.add(url)
                                all_items.append(it)
        finally:
            browser.close()


def get_full_game_list_for_category(category_key: str, include_versions: bool = True) -> list[dict]:
    resolved = resolve_category_key(category_key, GAME_CATEGORIES)
    if resolved in GAME_CATEGORIES:
        category_slug = GAME_CATEGORIES[resolved]
    else:
        category_slug = resolved.strip().lower().replace(" ", "-")
    base_url = BASE.rstrip("/") + "/" + category_slug
    all_items: list[dict] = []
    seen_urls: set[str] = set()
    if not include_versions:
        next_page_list: list[int] = [1]
        claimed: set[int] = set()
        merged_pages: set[int] = set()
        max_detected: list[bool] = [False]
        max_page_value: list[int] = [0]
        lock = Lock()
        headless = get_config().browser.headless
        workers_count = max(1, get_config().thread_pool.max_workers)
        threads = [
            Thread(
                target=_peek_page_worker,
                args=(
                    base_url,
                    next_page_list,
                    claimed,
                    merged_pages,
                    all_items,
                    seen_urls,
                    max_detected,
                    max_page_value,
                    lock,
                    headless,
                ),
            )
            for _ in range(workers_count)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return all_items
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=get_config().browser.headless, args=BROWSER_ARGS)
        try:
            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            page = context.new_page()
            pagenum = 1
            while True:
                page_url = f"{base_url}/{pagenum}" if pagenum > 1 else base_url
                page.goto(page_url, wait_until="load", timeout=TIMEOUT_NAVIGATION_MS)
                time.sleep(2)
                if _is_not_found_page(page):
                    break
                items = _parse_category_page_items(page, is_first_page=(pagenum == 1))
                for it in items:
                    url = it.get("game_url") or ""
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_items.append(it)
                pagenum += 1
            n = len(all_items)
            results: list[str] = [""] * n
            q: Queue = Queue()
            for i, it in enumerate(all_items):
                if it.get("game_url"):
                    q.put((i, it["game_url"]))
            max_workers = get_config().thread_pool.max_workers
            workers_count = min(max_workers, n)
            workers_count = max(1, workers_count)
            headless = get_config().browser.headless
            threads = [
                Thread(target=_fetch_versions_worker, args=(q, results, headless))
                for _ in range(workers_count)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            for i in range(n):
                if results[i]:
                    all_items[i]["version"] = results[i]
                elif all_items[i].get("game_url"):
                    all_items[i]["version"] = "latest"
            return all_items
        finally:
            browser.close()
