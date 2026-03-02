USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-infobars",
    "--disable-extensions",
    "--disable-gpu",
    "--window-size=1920,1080",
]

# Playwright timeouts (ms); used by crawlers for goto, wait_for_load_state, wait_for_selector.
TIMEOUT_NAVIGATION_MS = 30_000
TIMEOUT_LOAD_STATE_MS = 20_000
TIMEOUT_SELECTOR_MS = 15_000
TIMEOUT_SHORT_MS = 5_000
