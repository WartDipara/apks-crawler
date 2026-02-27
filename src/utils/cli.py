import argparse
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import Any

from config import get_config
from src.crawlers import APKPureCrawler
from src.logger import LogWriter, set_timer, calculate_time


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


def _staging_max_versions(index: list[dict]) -> dict[str, str]:
    # Return app_id -> max version in staging index (by semantic order).
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
    crawler: APKPureCrawler,
    app_id: str,
    list_version: str,
    slug: Any,
) -> None:
    logger.info(f"pull_fetch_start app_id={app_id} version={list_version}")
    try:
        crawler.fetch(app_id, list_version, slug=slug)
        logger.info(f"pull_fetch_done app_id={app_id} version={list_version}")
    except Exception as e:
        logger.info(f"pull_skip app_id={app_id} version={list_version} error={e!r}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="APK crawler (APKPure)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch one app by app_id, version and optional slug")
    fetch_parser.add_argument("app_id", help="Application package id (e.g. com.example.app)")
    fetch_parser.add_argument("--version", required=True, help="Version to fetch")
    fetch_parser.add_argument("--slug", help="APKPure slug for direct page (e.g. blob-hero-app)")

    peek_parser = subparsers.add_parser("peek", help="List games in a category (no download)")
    peek_parser.add_argument("category", help="Category key or display name (e.g. game_action, Action)")

    pull_parser = subparsers.add_parser("pull", help="Download games in a category; skip when staging version >= list version")
    pull_parser.add_argument("category", help="Category key or display name (e.g. game_action, Action)")

    return parser


def run(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    crawler: APKPureCrawler,
    logger: LogWriter,
    dispatch: Any = None,
) -> None:
    cmd = args.command
    cmd_args = f"command={cmd}"
    if cmd == "fetch":
        cmd_args += f" app_id={args.app_id} version={args.version}"
        if args.slug:
            cmd_args += f" slug={args.slug}"
    elif cmd in ("peek", "pull"):
        cmd_args += f" category={args.category}"

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
        logger.info(f"pull_start category={args.category} fetching game list")
        items = crawler.get_full_game_list(args.category)
        logger.info(f"pull category={args.category} count={len(items)}")
        staging_max = _staging_max_versions(crawler._staging.read_index())
        to_fetch: list[tuple[str, str, Any]] = []
        for item in items:
            app_id = item.get("app_id") or ""
            list_version = (item.get("version") or "").strip()
            if not list_version:
                continue
            slug = item.get("slug")
            stored = staging_max.get(app_id)
            if stored is not None and _version_ge(stored, list_version):
                continue
            to_fetch.append((app_id, list_version, slug))
        download_number = get_config().download_number
        if download_number > 0:
            to_fetch = to_fetch[:download_number]
            logger.info(f"pull to_fetch count={len(to_fetch)} limited_by_download_number={download_number}")
        else:
            logger.info(f"pull to_fetch count={len(to_fetch)}")
        if dispatch is not None:
            dispatch.start()
            futures: list[Future[None]] = []
            for app_id, list_version, slug in to_fetch:
                if dispatch.shutdown_requested:
                    break
                fut = dispatch.submit(_run_one_fetch, logger, crawler, app_id, list_version, slug)
                if fut is not None:
                    futures.append(fut)
            timeout_sec = dispatch.shutdown_timeout_seconds
            while futures and not dispatch.shutdown_requested:
                done, futures = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)
            if dispatch.shutdown_requested and futures:
                for f in futures:
                    f.cancel()
                wait(futures, timeout=timeout_sec)
        else:
            for app_id, list_version, slug in to_fetch:
                _run_one_fetch(logger, crawler, app_id, list_version, slug)

    duration = calculate_time(command_start, time.time())
    logger.info(f"command_end {cmd_args} duration_seconds={duration:.3f}")
