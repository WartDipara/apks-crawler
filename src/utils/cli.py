import argparse
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import Any
from config import get_config
from src.crawlers.base import BaseCrawler
from src.logger import LogWriter, set_timer, calculate_time

SOURCE_CHOICES = ("apkpure", "uptodown")

def _version_ge(v1: str, v2: str) -> bool:
    # True if v1 >= v2. Used for skip: skip when stored >= list_version; re-download only when list_version > stored (v1 > v2). When equal, no re-download.
    def parse(s: str) -> tuple[int, ...]:
        out = []
        for part in (s or "").strip().split("."):
            try:
                out.append(int(part))
            except ValueError:
                out.append(0)
        return tuple(out) if out else (0,)
    p1 = parse(v1)
    p2 = parse(v2)
    n = max(len(p1), len(p2))
    p1 = p1 + (0,) * (n - len(p1))
    p2 = p2 + (0,) * (n - len(p2))
    return p1 >= p2


def _index_max_versions(index: list[dict]) -> dict[str, str]:
    # Return app_id -> max version in platform index (by semantic order).
    def version_key(s: str) -> tuple[int, ...]:
        parts = []
        for x in (s or "").strip().split("."):
            try:
                parts.append(int(x))
            except ValueError:
                parts.append(0)
        return tuple(parts) if parts else (0,)
    by_app: dict[str, list[str]] = {}
    for e in index:
        aid = e.get("app_id")
        ver = (e.get("version") or "").strip()
        if not aid or not ver:
            continue
        by_app.setdefault(aid, []).append(ver)
    return {aid: max(vers, key=version_key) for aid, vers in by_app.items()}


def _run_one_fetch(
    logger: LogWriter,
    crawler: BaseCrawler,
    app_id: str,
    list_version: str,
    slug: Any,
    game_url: Any = None,
) -> bool:
    logger.info(f"pull_fetch_start app_id={app_id} version={list_version}")
    try:
        kwargs = {"slug": slug}
        if game_url is not None:
            kwargs["game_url"] = game_url
        resolved = crawler.fetch(app_id, list_version, **kwargs)
        version_log = resolved if resolved else list_version
        logger.info(f"pull_fetch_done app_id={app_id} version={version_log}")
        return resolved is not None
    except Exception as e:
        logger.info(f"pull_skip app_id={app_id} version={list_version} error={e!r}")
        return False

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="APK crawler (multi-source)")
    parser.add_argument(
        "--source",
        choices=SOURCE_CHOICES,
        required=True,
        help="Crawler source to use (e.g. apkpure, uptodown)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch one app by app_id, version and optional slug")
    fetch_parser.add_argument("app_id", help="Application package id (e.g. com.example.app)")
    fetch_parser.add_argument("--version", required=True, help="Version to fetch")
    fetch_parser.add_argument("--slug", help="APKPure slug for direct page (e.g. blob-hero-app)")

    peek_parser = subparsers.add_parser("peek", help="List games in a category (no download)")
    peek_parser.add_argument("category", help="Category key or display name (e.g. game_action, Action)")

    pull_parser = subparsers.add_parser(
        "pull",
        help="Download games in one or more categories; each category gets up to download_number downloads (skips not counted)",
    )
    pull_parser.add_argument(
        "categories",
        nargs="+",
        help="Category keys or display names (e.g. Action Kids Adventure)",
    )

    return parser


def run(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    crawler: BaseCrawler,
    logger: LogWriter,
    dispatch: Any = None,
) -> None:
    cmd = args.command
    cmd_args = f"command={cmd} source={getattr(args, 'source', '')}"
    if cmd == "fetch":
        cmd_args += f" app_id={args.app_id} version={args.version}"
        if args.slug:
            cmd_args += f" slug={args.slug}"
    elif cmd == "peek":
        cmd_args += f" category={args.category}"
    elif cmd == "pull":
        cmd_args += f" categories={args.categories}"

    logger.info(f"command_start {cmd_args}")
    command_start = set_timer("command_start")

    if cmd == "fetch":
        kwargs = {"slug": args.slug} if args.slug else {}
        crawler.fetch(args.app_id, args.version, **kwargs)
    elif cmd == "peek":
        items = crawler.get_full_game_list(args.category)
        logger.info(f"peek category={args.category} count={len(items)}")
        for item in items:
            line = f"{item.get('app_id')}\t{item.get('version')}\t{item.get('slug', '')}"
            logger.info(f"peek {line}")
    elif cmd == "pull":
        download_number = get_config().download_number
        if download_number > 0:
            logger.info(f"pull will download up to {download_number} per category (skips not counted)")
        for category in args.categories:
            logger.info(f"pull_start category={category} fetching game list")
            items = crawler.get_category_game_list(category)
            logger.info(f"pull category={category} count={len(items)}")
            for i, item in enumerate(items):
                logger.info(
                    f"pull_list category={category} index={i} app_id={item.get('app_id', '')} version={item.get('version', '')} "
                    f"slug={item.get('slug', '')} game_url={item.get('game_url', '')}"
                )
            index_max = _index_max_versions(crawler._storage.read_index())
            to_fetch: list[tuple[str, str, Any, Any]] = []
            for item in items:
                app_id = item.get("app_id") or ""
                list_version = (item.get("version") or "").strip()
                slug = item.get("slug")
                game_url = item.get("game_url")
                if list_version:
                    stored = index_max.get(app_id)
                    if stored is not None and _version_ge(stored, list_version):
                        logger.info(f'skip game="{app_id}" reason="Latest version already installed"')
                        continue
                to_fetch.append((app_id, list_version, slug, game_url))
            logger.info(f"pull category={category} to_fetch count={len(to_fetch)}")
            if dispatch is not None:
                dispatch.start()
                downloaded_count = 0
                it = iter(to_fetch)
                in_flight: list[Future[bool]] = []
                timeout_sec = dispatch.shutdown_timeout_seconds
                while True:
                    while len(in_flight) < (get_config().thread_pool.max_workers) and not dispatch.shutdown_requested:
                        if download_number > 0 and downloaded_count >= download_number:
                            break
                        try:
                            app_id, list_version, slug, game_url = next(it)
                        except StopIteration:
                            break
                        fut = dispatch.submit(_run_one_fetch, logger, crawler, app_id, list_version, slug, game_url)
                        if fut is not None:
                            in_flight.append(fut)
                    if not in_flight:
                        break
                    done, pending = wait(in_flight, timeout=1.0, return_when=FIRST_COMPLETED)
                    in_flight = list(pending)
                    for f in done:
                        try:
                            if not dispatch.shutdown_requested and f.result() is True:
                                downloaded_count += 1
                        except Exception:
                            pass
                        if dispatch.shutdown_requested:
                            break
                    if dispatch.shutdown_requested:
                        for f in in_flight:
                            f.cancel()
                        wait(in_flight, timeout=timeout_sec)
                        break
                    if download_number > 0 and downloaded_count >= download_number:
                        for f in in_flight:
                            f.cancel()
                        wait(in_flight, timeout=timeout_sec)
                        break
            else:
                downloaded_count = 0
                for app_id, list_version, slug, game_url in to_fetch:
                    if download_number > 0 and downloaded_count >= download_number:
                        break
                    if _run_one_fetch(logger, crawler, app_id, list_version, slug, game_url):
                        downloaded_count += 1
            if dispatch is not None and dispatch.shutdown_requested:
                break

    duration = calculate_time(command_start, time.time())
    logger.info(f"command_end {cmd_args} duration_seconds={duration:.3f}")
