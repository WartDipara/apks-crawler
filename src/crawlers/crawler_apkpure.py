import re
import time
from pathlib import Path
from playwright.sync_api import sync_playwright
from config import get_config
from src.crawlers.base import BaseCrawler
from src.crawlers.common import (
    BROWSER_ARGS,
    USER_AGENT,
    TIMEOUT_LOAD_STATE_MS,
    TIMEOUT_NAVIGATION_MS,
    TIMEOUT_SELECTOR_MS,
    TIMEOUT_SHORT_MS,
)
from src.exception import CrawlerDownloadError, CrawlerPageError
from src.utils import hash_file

BASE = "https://apkpure.com"
SEARCH_URL = "https://apkpure.com/search"
STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    window.chrome = { runtime: {} };
}
"""
DOWNLOAD_BTN_SELECTORS = [
    "a.download-btn[href*='d.apkpure.com']",
    "a.download-start-btn[href*='d.apkpure.com']",
    "a.download-btn",
]
AD_WAIT_TIME = 2.5
MAX_BACK_ATTEMPTS = 2
LOAD_MORE_SELECTORS = [
    "a.show-more",
    "a.loadmore",
    "button:has-text('Load More')",
    "a:has-text('Load More')",
    "button:has-text('Show More')",
    "a:has-text('Show More')",
    "[class*='load-more']",
    "[class*='show-more']",
]
GAME_CATEGORIES = {
    "Action": "game_action",
    "Adventure": "game_adventure",
    "Arcade": "game_arcade",
    "Board": "game_board",
    "Card": "game_card",
    "Casual": "game_casual",
    "Educational": "game_educational",
    "Music": "game_music",
    "Puzzle": "game_puzzle",
    "Racing": "game_racing",
    "RolePlaying": "game_role_playing",
    "Simulation": "game_simulation",
    "Sports": "game_sports",
    "Strategy": "game_strategy",
    "Trivia": "game_trivia",
    "Word": "game_word",
    "Family": "game_family",
}


def _close_popup_dialogs(page) -> None:
    for _ in range(3):
        closed = False
        for selector in [
            "button:has-text('Close')", "a:has-text('Close')", "[aria-label='Close']",
            "[class*='close-btn']", "[class*='modal-close']", ".popup-close",
        ]:
            try:
                for i in range(page.locator(selector).count()):
                    el = page.locator(selector).nth(i)
                    if el.is_visible():
                        el.click(force=True)
                        time.sleep(0.3)
                        closed = True
            except Exception:
                continue
        if not closed:
            break
        time.sleep(0.3)


def _is_download_button_blocked(page) -> bool:
    # Whether the download button is covered by iframe or ad overlay.
    for sel in DOWNLOAD_BTN_SELECTORS:
        btn = page.locator(sel).first
        if btn.count() == 0:
            continue
        try:
            btn.scroll_into_view_if_needed()
            time.sleep(0.3)
            box = btn.bounding_box()
            if not box:
                continue
            x = box["x"] + box["width"] / 2
            y = box["y"] + box["height"] / 2
            viewport = page.viewport_size
            if viewport:
                x = max(0, min(x, viewport["width"] - 1))
                y = max(0, min(y, viewport["height"] - 1))
            result = page.evaluate(
                """([x, y]) => {
                    const el = document.elementFromPoint(x, y);
                    if (!el) return { blocked: false };
                    const btn = document.querySelector('a.download-btn[href*="d.apkpure.com"], a.download-start-btn[href*="d.apkpure.com"]');
                    if (btn && (el === btn || btn.contains(el) || el.contains(btn))) return { blocked: false };
                    const style = getComputedStyle(el);
                    const z = parseInt(style.zIndex) || 0;
                    const fixed = style.position === 'fixed' && z > 1000;
                    return { blocked: el.tagName === 'IFRAME' || fixed };
                }""",
                [x, y],
            )
            if result.get("blocked"):
                return True
            return False
        except Exception:
            continue
    return False


def _handle_download_page_ads(page, download_page_url: str) -> bool:
    # Wait for ad/overlay; if button is blocked, go back and re-enter to try to dismiss.
    time.sleep(AD_WAIT_TIME)
    if not _is_download_button_blocked(page):
        return True
    for attempt in range(MAX_BACK_ATTEMPTS):
        try:
            page.go_back(timeout=TIMEOUT_SHORT_MS)
            time.sleep(0.5)
            if "/download" in page.url and "apkpure" in page.url:
                time.sleep(1)
                if not _is_download_button_blocked(page):
                    return True
            page.goto(download_page_url, wait_until="domcontentloaded", timeout=TIMEOUT_SELECTOR_MS)
            time.sleep(AD_WAIT_TIME)
            if not _is_download_button_blocked(page):
                return True
        except Exception:
            pass
    return False


def _find_download_button(page):
    for sel in DOWNLOAD_BTN_SELECTORS:
        btn = page.locator(sel).first
        if btn.count() > 0:
            return btn
    return None


def _extract_version_from_page(page) -> str | None:
    try:
        title = page.title()
        if title:
            match = re.search(r"(\d+(?:\.\d+)+)", title)
            if match:
                return match.group(1)
    except Exception:
        pass
    try:
        ver_el = page.locator(".info .name, .info-top .name").first
        if ver_el.count() > 0:
            text = ver_el.inner_text()
            if text:
                match = re.search(r"(\d+(?:\.\d+)+)", text.strip())
                if match:
                    return match.group(1)
    except Exception:
        pass
    try:
        ver_el = page.locator("text=/\\d+\\.\\d+/").first
        if ver_el.count() > 0:
            text = ver_el.inner_text()
            if text:
                match = re.search(r"(\d+(?:\.\d+)+)", text.strip())
                if match:
                    return match.group(1)
    except Exception:
        pass
    return None


def _category_context(browser):
    """Same context as fetch: user_agent, viewport, stealth, so category page renders like normal browser."""
    context = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
    )
    context.add_init_script(STEALTH_JS)
    return context


def _click_latest_tab(page) -> None:
    """Switch category page to 'Latest update' tab if present."""
    latest_tab = (
        page.get_by_role("tab", name=re.compile(r"latest\s*update", re.I))
        .or_(page.get_by_text(re.compile(r"latest\s*update", re.I)))
        .or_(page.locator("a[href*='latest']"))
        .first
    )
    if latest_tab.count():
        latest_tab.click()
        page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_LOAD_STATE_MS)
    time.sleep(2)


# Category list: live apkpure.com uses .grid-row + a.grid-item-title (href=https://apkpure.com/slug/com.pkg) and [data-dt-pkg]
CATEGORY_LIST_SELECTOR = "div.grid-row"
CATEGORY_GRID_ROW = "div.grid-row"


def _href_to_app_slug(href: str) -> tuple[str, str] | None:
    """Parse APKPure app link to (app_id, slug). Supports https://apkpure.com/slug/com.pkg or /slug/com.pkg."""
    if "/download" in href or not href.strip():
        return None
    path = href.split("?")[0].strip().rstrip("/")
    if "apkpure.com/" in path:
        path = path.split("apkpure.com/", 1)[-1]
    if path.startswith("/"):
        path = path[1:]
    parts = [x for x in path.split("/") if x]
    if len(parts) < 2:
        return None
    if "." in parts[-1]:
        return (parts[-1], parts[-2])
    if len(parts) >= 3 and "." in parts[-2]:
        return (parts[-2], parts[-1])
    return None


def _parse_category_page_items(page) -> list[dict]:
    """Extract game entries. Prefer current apkpure layout: .grid-row (data-dt-pkg + link) and a.grid-item-title."""
    items: list[dict] = []
    seen: set[tuple[str, str]] = set()
    # Primary: div.grid-row with data-dt-pkg and a.grid-item-title or first app link
    rows = page.locator(CATEGORY_GRID_ROW)
    for i in range(rows.count()):
        try:
            row = rows.nth(i)
            pkg = row.get_attribute("data-dt-pkg")
            if pkg:
                link = row.locator("a.grid-item-title").first
                if link.count() == 0:
                    link = row.locator("a[href*='apkpure.com/'][href*='/com.']").first
                if link.count() == 0:
                    continue
                href = link.get_attribute("href") or ""
                parsed = _href_to_app_slug(href)
                if not parsed:
                    continue
                _, slug = parsed
                if (pkg, slug) in seen:
                    continue
                seen.add((pkg, slug))
                ver = row.get_attribute("data-dt-version") or "latest"
                items.append({"app_id": pkg, "version": ver or "latest", "slug": slug})
                continue
            # No data-dt-pkg: get from link
            link = row.locator("a.grid-item-title, a[href*='apkpure.com/'][href*='/com.']").first
            if link.count() == 0:
                continue
            href = link.get_attribute("href") or ""
            parsed = _href_to_app_slug(href)
            if not parsed or parsed in seen:
                continue
            pkg, slug = parsed
            seen.add((pkg, slug))
            ver = row.get_attribute("data-dt-version") or "latest"
            items.append({"app_id": pkg, "version": ver or "latest", "slug": slug})
        except Exception:
            continue
    if items:
        return items
    # Fallback: any a.grid-item-title or a[href*="apkpure.com/"][href*="/com."] not download
    for selector in [
        "a.grid-item-title",
        'a[href*="apkpure.com/"][href*="/com."]',
    ]:
        game_links = page.locator(selector)
        for i in range(game_links.count()):
            try:
                card = game_links.nth(i)
                href = card.get_attribute("href") or ""
                if "/download" in href:
                    continue
                parsed = _href_to_app_slug(href)
                if not parsed or parsed in seen:
                    continue
                pkg, slug = parsed
                seen.add((pkg, slug))
                parent = card.locator("xpath=..")
                version_el = parent.locator("[data-dt-version], [class*='version'], .p1").first
                version = version_el.get_attribute("data-dt-version") if version_el.count() else None
                if not version:
                    version = version_el.inner_text().strip() if version_el.count() else "latest"
                items.append({"app_id": pkg, "version": version or "latest", "slug": slug})
            except Exception:
                continue
    return items


def _scroll_to_bottom(page) -> None:
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(0.5)


def _click_load_more(page) -> bool:
    for selector in LOAD_MORE_SELECTORS:
        btn = page.locator(selector).first
        if btn.count() == 0:
            continue
        try:
            if not btn.is_visible():
                continue
            btn.scroll_into_view_if_needed()
            time.sleep(0.5)
            btn.click()
            time.sleep(2)
            return True
        except Exception:
            continue
    return False


class APKPureCrawler(BaseCrawler):
    def __init__(self, storage, logger):
        super().__init__("apkpure", storage, logger)

    def resolve_download_url(self, app_id: str, version: str, slug: str | None = None, **kwargs: object) -> tuple[str, dict[str, str]]:
    # For compatibility only: returns direct URL. Prefer fetch() which uses Playwright expect_download + click.
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=get_config().browser.headless, args=BROWSER_ARGS)
            try:
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                )
                context.add_init_script(STEALTH_JS)
                page = context.new_page()
                if slug:
                    href = f"{BASE.rstrip('/')}/{slug}/{app_id}"
                else:
                    page.goto(f"{SEARCH_URL}?q={app_id}", wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                    page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                    time.sleep(2)
                    app_link = page.locator(f'a[href*="{app_id}"]').first
                    if not app_link.count():
                        app_link = page.locator('a[href*="/game/"][href*="."], a[href^="/"][href*="/"][href*="."]').first
                    if not app_link.count():
                        raise CrawlerPageError("no app page found", source_name=self.source_name, app_id=app_id)
                    href = app_link.get_attribute("href") or ""
                    if href.startswith("/"):
                        href = BASE.rstrip("/") + href
                download_page_url = href.rstrip("/") + "/download"
                page.goto(download_page_url, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                download_btn = _find_download_button(page)
                if not download_btn:
                    raise CrawlerPageError("no download-btn on download page", source_name=self.source_name, app_id=app_id)
                dl_href = download_btn.get_attribute("href") or ""
                if not dl_href:
                    raise CrawlerPageError("download-btn has no href", source_name=self.source_name, app_id=app_id)
                if dl_href.startswith("/"):
                    dl_href = BASE.rstrip("/") + dl_href
                page.goto(dl_href, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                final_url = page.url
                cookies = context.cookies()
                cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
                ua = page.evaluate("() => navigator.userAgent")
                return (final_url, {"Cookie": cookie_str, "User-Agent": ua})
            finally:
                browser.close()

    def fetch(self, app_id: str, version: str, **kwargs: object) -> str | None:
        slug = kwargs.get("slug")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=get_config().browser.headless, args=BROWSER_ARGS)
            try:
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                )
                context.add_init_script(STEALTH_JS)
                page = context.new_page()
                if slug:
                    download_page_url = f"{BASE.rstrip('/')}/{slug}/{app_id}/download"
                else:
                    page.goto(f"{SEARCH_URL}?q={app_id}", wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                    page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                    time.sleep(2)
                    app_link = page.locator(f'a[href*="{app_id}"]').first
                    if not app_link.count():
                        app_link = page.locator('a[href*="/game/"][href*="."], a[href^="/"][href*="/"][href*="."]').first
                    if not app_link.count():
                        raise CrawlerPageError("no app page found", source_name=self.source_name, app_id=app_id)
                    href = app_link.get_attribute("href") or ""
                    if href.startswith("/"):
                        href = BASE.rstrip("/") + href
                    download_page_url = href.rstrip("/") + "/download"
                page.goto(download_page_url, wait_until="domcontentloaded", timeout=TIMEOUT_NAVIGATION_MS)
                page.wait_for_load_state("load", timeout=TIMEOUT_LOAD_STATE_MS)
                time.sleep(2)
                _close_popup_dialogs(page)
                if not _handle_download_page_ads(page, download_page_url):
                    raise CrawlerDownloadError("download button blocked by ad after retries", source_name=self.source_name, app_id=app_id)
                download_btn = _find_download_button(page)
                if not download_btn:
                    raise CrawlerPageError("no download-btn on download page", source_name=self.source_name, app_id=app_id)
                resolved_version = _extract_version_from_page(page) or version
                download_btn.scroll_into_view_if_needed()
                time.sleep(0.5)
                filename = self._apk_filename(app_id, resolved_version)
                dest_path = self._storage.apk_path(filename)
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                timeout_ms = int((get_config().download.timeout_seconds or 300) * 1000)
                with page.expect_download(timeout=timeout_ms) as download_ctx:
                    download_btn.click()
                download = download_ctx.value
                download.save_as(dest_path)
                if download.failure():
                    raise CrawlerDownloadError(str(download.failure()), source_name=self.source_name, app_id=app_id)
                md5_hex = hash_file(Path(dest_path))
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
        return discover_latest_in_category(category)

    def get_full_game_list(self, category: str) -> list[dict]:
        return get_full_game_list_for_category(category)


def discover_latest_in_category(category_key: str) -> list[dict]:
    if category_key in GAME_CATEGORIES:
        category_key = GAME_CATEGORIES[category_key]
    base_url = f"{BASE.rstrip('/')}/{category_key}"
    headless = get_config().browser.headless
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=BROWSER_ARGS)
        try:
            context = _category_context(browser)
            page = context.new_page()
            page.goto(base_url, wait_until="load", timeout=TIMEOUT_NAVIGATION_MS)
            time.sleep(2)
            _click_latest_tab(page)
            try:
                page.wait_for_selector(CATEGORY_LIST_SELECTOR, timeout=TIMEOUT_SELECTOR_MS)
            except Exception:
                try:
                    page.wait_for_selector("a[href*='/game/'], a[href^='/'][href*='/']", timeout=TIMEOUT_SHORT_MS)
                except Exception:
                    pass
            return _parse_category_page_items(page)
        finally:
            browser.close()


# Get full game list; count from config.apkpure.load_more_count(0 is no limit). 
# Flow: category -> Latest tab -> scroll to bottom -> [parse items -> scroll to bottom -> click Load More -> scroll to bottom] repeat.
def get_full_game_list_for_category(category_key: str) -> list[dict]:
    if category_key in GAME_CATEGORIES:
        category_key = GAME_CATEGORIES[category_key]
    base_url = f"{BASE.rstrip('/')}/{category_key}"
    max_load_more = get_config().apkpure.load_more_count
    all_items: list[dict] = []
    seen_app_ids: set[str] = set()
    load_more_count = 0
    headless = get_config().browser.headless
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, args=BROWSER_ARGS)
        try:
            context = _category_context(browser)
            page = context.new_page()
            page.goto(base_url, wait_until="load", timeout=TIMEOUT_NAVIGATION_MS)
            time.sleep(2)
            _click_latest_tab(page)
            try:
                page.wait_for_selector(CATEGORY_LIST_SELECTOR, timeout=TIMEOUT_SELECTOR_MS)
            except Exception:
                try:
                    page.wait_for_selector("a[href*='/game/'], a[href^='/'][href*='/']", timeout=TIMEOUT_SHORT_MS)
                except Exception:
                    pass
            _scroll_to_bottom(page)
            while True:
                items = _parse_category_page_items(page)
                for it in items:
                    aid = it.get("app_id") or ""
                    if aid and aid not in seen_app_ids:
                        seen_app_ids.add(aid)
                        all_items.append(it)
                if max_load_more and load_more_count >= max_load_more:
                    break
                _scroll_to_bottom(page)
                if not _click_load_more(page):
                    break
                load_more_count += 1
                _scroll_to_bottom(page)
            return all_items
        finally:
            browser.close()
