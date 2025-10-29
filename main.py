import os, json, sys, time
from decimal import Decimal, ROUND_DOWN

import gspread
from google.oauth2 import service_account
from alpaca_trade_api import REST, TimeFrame  # TimeFrame not used, but fine to keep

SHEET_NAME = os.getenv("SHEET_NAME", "Active-Investing")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Alpaca Integration")
SHEET_ID = os.getenv("SHEET_ID")  # <-- NEW: prefer opening by key to avoid Drive scope needs

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

BUY_FRACTION = Decimal("0.07")
MIN_NOTIONAL = Decimal(os.getenv("MIN_NOTIONAL", "1"))

def die(msg, code=1):
    print(msg, file=sys.stderr); sys.exit(code)

def get_gspread_client():
    if not GOOGLE_CREDS_JSON:
        die("Missing GOOGLE_CREDS_JSON env var containing Service Account JSON.")
    try:
        info = json.loads(GOOGLE_CREDS_JSON)
    except Exception as e:
        die(f"GOOGLE_CREDS_JSON is not valid JSON: {e}")
    # IMPORTANT: include Drive read-only so open(<title>) can search by name.
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def get_alpaca():
    if not (ALPACA_API_KEY and ALPACA_SECRET_KEY and ALPACA_BASE_URL):
        die("Missing one of ALPACA_API_KEY / ALPACA_SECRET_KEY / ALPACA_BASE_URL.")
    return REST(ALPACA_API_KEY, ALPACA_SECRET_KEY, base_url=ALPACA_BASE_URL, api_version="v2")

def decimal_usd(x) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

def main():
    gc = get_gspread_client()

    # Prefer opening by spreadsheet ID (no Drive scope needed)
    try:
        if SHEET_ID:
            sh = gc.open_by_key(SHEET_ID)
        else:
            sh = gc.open(SHEET_NAME)  # requires drive.readonly to list/search
    except Exception as e:
        die(f"Failed to open Google Sheet. Hint: set SHEET_ID env or share sheet with the service account. Underlying error: {e}")

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except Exception as e:
        die(f"Worksheet '{WORKSHEET_NAME}' not found: {e}")

    api = get_alpaca()
    account = api.get_account()
    buying_power = decimal_usd(account.buying_power)

    col_a = ws.col_values(1)
    if not col_a:
        print("No data found in column A; nothing to do."); return

    start_row = 2
    max_row = len(col_a)
    tickers_to_process = []
    for idx in range(start_row - 1, max_row):
        ticker = (col_a[idx] or "").strip()
        if ticker:
            tickers_to_process.append((idx + 1, ticker.upper()))

    if not tickers_to_process:
        print("No tickers listed below the header; nothing to do."); return

    print(f"Found {len(tickers_to_process)} ticker(s): {[t for _, t in tickers_to_process]}")

    for row_idx, ticker in tickers_to_process:
        try:
            account = api.get_account()
            buying_power = decimal_usd(account.buying_power)
            notional = (buying_power * BUY_FRACTION).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if notional < MIN_NOTIONAL:
                print(f"Skipping {ticker} — notional ${notional} under minimum ${MIN_NOTIONAL}.")
                ws.update_cell(row_idx, 3, ticker)
                ws.update_cell(row_idx, 4, f"SKIPPED (notional ${notional})")
                continue

            order = api.submit_order(
                symbol=ticker, side="buy", type="market", time_in_force="day", notional=float(notional)
            )

            ws.update_cell(row_idx, 3, ticker)
            ws.update_cell(row_idx, 4, f"{notional}")
            print(f"Submitted BUY {ticker} @ ${notional}. Order ID: {order.id}")

        except Exception as e:
            err = f"ERROR: {e}"
            print(f"{ticker}: {err}", file=sys.stderr)
            try:
                ws.update_cell(row_idx, 3, ticker)
                ws.update_cell(row_idx, 4, err)
            except Exception:
                pass
        time.sleep(0.4)

    try:
        clear_range = f"A{start_row}:A{max_row}"
        ws.batch_clear([clear_range])
        print(f"Cleared input range {clear_range}.")
    except Exception:
        blank_block = [[""] for _ in range(max_row - start_row + 1 if max_row >= start_row else 0)]
        if blank_block:
            ws.update(f"A{start_row}:A{max_row}", blank_block)
            print(f"Cleared input range {clear_range} via update fallback.")

if __name__ == "__main__":
    main()
