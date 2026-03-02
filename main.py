# Launch the crawler (single entrypoint + source selection)
import sys
import time
from pathlib import Path

from config import get_config
from src.crawlers import APKPureCrawler, UptodownCrawler
from src.dispatch import cleanup_partial_files, get_dispatch, init_dispatch
from src.logger import LogWriter, set_timer, calculate_time
from src.storage import PlatformStorage, get_paths
from src.utils.cli import SOURCE_CHOICES, build_parser, run


def _crawler_for_source(source: str, storage: PlatformStorage, logger: LogWriter):
    if source == "apkpure":
        return APKPureCrawler(storage, logger)
    if source == "uptodown":
        return UptodownCrawler(storage, logger)
    raise ValueError(f"Unknown source: {source}. Choose from {SOURCE_CHOICES}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = get_config(Path("config.json"))
    paths = get_paths(config, platform=args.source)
    paths.ensure_dirs()
    storage = PlatformStorage(paths)
    logger = LogWriter(paths.logs_dir)

    init_dispatch(config)
    dispatch = get_dispatch()
    dispatch.register_signal_handlers()

    script_start = set_timer("script_start")
    logger.info("script_start")
    logger.info(f"source={args.source}")

    crawler = _crawler_for_source(args.source, storage, logger)
    try:
        run(parser, args, crawler, logger, dispatch=dispatch)
        cleanup_partial_files(storage)
        dispatch.shutdown()
    except KeyboardInterrupt:
        logger.info("interrupted cleanup_start")
        cleanup_partial_files(storage)
        dispatch.shutdown(wait=False, cancel_futures=True)
        logger.info("interrupted cleanup_end")
        sys.exit(0)

    duration = calculate_time(script_start, time.time())
    logger.info(f"script_end duration_seconds={duration:.3f}")


if __name__ == "__main__":
    main()
    print("Finished. For more details, check the logs in the data/logs directory.")