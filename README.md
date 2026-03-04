# Tutorial

## How to run?

Use one entrypoint and choose the source with `--source`:

```bash
python main.py --source <apkpure|uptodown> <command> [args...]
```

**Requirements:** Python 3.x, dependencies in `requirements.txt`. For browser-based crawl, activate your env (e.g. `conda activate OvO`) so Playwright is available.

**Config:** Edit `config.json` to set storage root, `download_number`, `browser.headless`, etc.

---

## Commands

| Command | Meaning | What you get |
|--------|--------|--------------|
| **peek** `<category>` | List games in a category (no download). | Logs under `data/logs/`: one line per game with `app_id`, `version`, `slug`/`game_url`. No files written to `data/<platform>/apks/`. |
| **pull** `<category> [category ...]` | Download games from one or more categories. | For each category, the script discovers the game list, compares with the platform index, then downloads up to `download_number` new/updated APKs. Results: `data/<platform>/index.json` and `data/<platform>/apks/`. Logs record each `pull_fetch_start` / `pull_fetch_done` / `pull_skip`. |
| **fetch** `<app_id>` `--version` `<version>` `[--slug ...]` | Download one app by id and version. | Same as one item from pull: APK under `data/<platform>/apks/` and one entry appended to `data/<platform>/index.json`. Logs under `data/logs/`. |

---

## Examples

```bash
# List Action games from APKPure (no download)
python main.py --source apkpure peek Action

# List Kids games from Uptodown (no download)
python main.py --source uptodown peek Kids

# Download up to download_number games per category (from config) for APKPure Action and Sports
python main.py --source apkpure pull Action Sports

# Download from Uptodown Kids category
python main.py --source uptodown pull Kids

# Fetch a single app (APKPure)
python main.py --source apkpure fetch com.example.game --version 1.0.0 --slug my-game-slug
```

---

## Output locations

- **Logs:** `data/logs/` (by date). All runs and commands are logged (script_start, command_start, peek/pull/fetch lines, script_end, duration).
- **APKPure data:** `data/apkpure/index.json`, `data/apkpure/apks/`.
- **Uptodown data:** `data/uptodown/index.json`, `data/uptodown/apks/`.

Each platform’s index is a JSON list of entries with `app_id`, `version`, `path` (APK filename), `hash`, etc. Already-installed latest versions are skipped and not counted toward `download_number`.

---

## Categories (for peek / pull)

Use one of the category names below as the `<category>` argument for **peek** and **pull**. Each source has its own list; use the name exactly as shown (or the slug in parentheses where different).

### APKPure (`--source apkpure`)

| Input (category name) |
|------------------------|
| Action |
| Adventure |
| Arcade |
| Board |
| Card |
| Casual |
| Educational |
| Music |
| Puzzle |
| Racing |
| RolePlaying |
| Simulation |
| Sports |
| Strategy |
| Trivia |
| Word |
| Family |

Example: `python main.py --source apkpure peek Action` or `python main.py --source apkpure pull Sports Puzzle`.

### Uptodown (`--source uptodown`)

| Input (category name) |
|------------------------|
| RPG |
| Strategy |
| Casual |
| Emulator |
| Arcade |
| Puzzle |
| Sports |
| Racing/Sim |
| Action/Adventure |
| Other |
| Platform |
| Kids |
| Card |
| NewReleases |
| TopDownloads |

- **NewReleases**: latest released games.  
- **TopDownloads**: most downloaded games.

Example: `python main.py --source uptodown peek Kids` or `python main.py --source uptodown pull RPG Sports`.
