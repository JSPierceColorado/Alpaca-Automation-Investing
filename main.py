import os, json, sys, time
from decimal import Decimal, ROUND_DOWN

import gspread
from google.oauth2 import service_account
from alpaca_trade_api import REST
from alpaca_trade_api.rest import APIError


# --- CONFIGURATION ---
SHEET_NAME = os.getenv("SHEET_NAME", "Active-Investing")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Alpaca Integration")
SHEET_ID = os.getenv("SHEET_ID")

ALPACA_API_KEY = (os.getenv("ALPACA_API_KEY") or "").strip()
ALPACA_SECRET_KEY = (os.getenv("ALPACA_SECRET_KEY") or "").strip()
ALPACA_BASE_URL = (os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets") or "").strip()

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

BUY_FRACTION = Decimal("0.07")  # 7% of buying power per ticker
MIN_NOTIONAL = Decimal(os.getenv("MIN_NOTIONAL", "1"))  # $1 min trade size


# --- HELPERS ---
def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)


def validate_alpaca_env():
    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        die("Missing ALPACA_API_KEY or ALPACA_SECRET_KEY.")
    if not ALPACA_BASE_URL.startswith("https://"):
        die(f"ALPACA_BASE_URL looks wrong: '{ALPACA_BASE_URL}'")

    prefix = ALPACA_API_KEY[:2].upper()
    if "paper-api.alpaca.markets" in ALPACA_BASE_URL and prefix != "PK":
        print(f"⚠️  Warning: Key prefix '{prefix}' may not match paper API.", file=sys.stderr)
    if "api.alpaca.markets" in ALPACA_BASE_URL and prefix != "AK":
        print(f"⚠️  Warning: Key prefix '{prefix}' may not match live API.", file=sys.stderr)


def get_alpaca():
    validate_alpaca_env()
    print(f"✅ Connecting to Alpaca @ {ALPACA_BASE_URL} using key …{ALPACA_API_KEY[-4:]}")
    return REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL, api_version="v2")


def get_gspread_client():
    if not GOOGLE_CREDS_JSON:
        die("Missing GOOGLE_CREDS_JSON env var containing Service Account JSON.")
    try:
        info = json.loads(GOOGLE_CREDS_JSON)
    except Exception as e:
        die(f"GOOGLE_CREDS_JSON is not valid JSON: {e}")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def decimal_usd(x) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def first_empty_row(ws, col: int, start_row: int = 2) -> int:
    vals = ws.col_values(col)
    return max(start_row, len(vals) + 1)


# --- MAIN LOGIC ---
def main():
    gc = get_gspread_client()

    # open the spreadsheet
    try:
        if SHEET_ID:
            sh = gc.open_by_key(SHEET_ID)
        else:
            sh = gc.open(SHEET_NAME)
        ws = sh.worksheet(WORKSHEET_NAME)
    except Exception as e:
        die(f"❌ Failed to open Google Sheet: {e}")

    api = get_alpaca()
    try:
        account = api.get_account()
        buying_power = decimal_usd(account.buying_power)
        print(f"💰 Current buying power: ${buying_power}")
    except APIError as e:
        die(f"❌ Alpaca auth failed: {e}. Check keys and ALPACA_BASE_URL.")

    # read tickers from column A
    col_a = ws.col_values(1)
    if not col_a:
        print("No data found in column A; nothing to do.")
        return

    start_row = 2
    max_row = len(col_a)
    tickers_to_process = []
    for idx in range(start_row - 1, max_row):
        t = (col_a[idx] or "").strip()
        if t:
            tickers_to_process.append((idx + 1, t.upper()))

    if not tickers_to_process:
        print("No tickers listed below the header; nothing to do.")
        return

    print(f"📈 Found {len(tickers_to_process)} ticker(s): {[t for _, t in tickers_to_process]}")

    next_log_row = first_empty_row(ws, col=3, start_row=2)
    log_rows = []  # [[ticker, cost/status]]

    for _, ticker in tickers_to_process:
        try:
            account = api.get_account()
            buying_power = decimal_usd(account.buying_power)
            notional = (buying_power * BUY_FRACTION).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            if notional < MIN_NOTIONAL:
                print(f"Skipping {ticker} — notional ${notional} < ${MIN_NOTIONAL}")
                log_rows.append([ticker, f"SKIPPED (notional ${notional})"])
                continue

            order = api.submit_order(
                symbol=ticker, side="buy", type="market", time_in_force="day", notional=float(notional)
            )

            print(f"✅ Submitted BUY {ticker} for ${notional} (Order ID: {order.id})")
            log_rows.append([ticker, f"{notional}"])

        except Exception as e:
            err = f"ERROR: {e}"
            print(f"{ticker}: {err}", file=sys.stderr)
            log_rows.append([ticker, err])

        time.sleep(0.4)

    # write logs to the next open rows in C:D
    if log_rows:
        first_row = first_empty_row(ws, col=3, start_row=2)
        last_row = first_row + len(log_rows) - 1
        ws.update(f"C{first_row}:D{last_row}", log_rows, value_input_option="RAW")
        print(f"📝 Logged {len(log_rows)} rows to C{first_row}:D{last_row}")

    # clear all of column A
    try:
        ws.batch_clear(["A:A"])
        print("🧹 Cleared entire column A.")
    except Exception as e:
        print(f"batch_clear failed ({e}); trying fallback.")
        rows = len(ws.get_all_values()) or 1000
        blanks = [[""] for _ in range(rows)]
        ws.update(f"A1:A{rows}", blanks)
        print("🧹 Cleared entire column A via fallback.")


if __name__ == "__main__":
    main()
