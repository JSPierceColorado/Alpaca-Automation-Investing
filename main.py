import os, json, sys, time
from decimal import Decimal, ROUND_DOWN

import gspread
from google.oauth2 import service_account
from alpaca_trade_api import REST, TimeFrame

SHEET_NAME = os.getenv("SHEET_NAME", "Active-Investing")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Alpaca Integration")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
# For live, set ALPACA_BASE_URL=https://api.alpaca.markets
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL", "https://paper-api.alpaca.markets")

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON")

BUY_FRACTION = Decimal("0.07")  # 7% of current buying power per ticker
MIN_NOTIONAL = Decimal(os.getenv("MIN_NOTIONAL", "1"))  # $1 floor just in case

def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def get_gspread_client():
    if not GOOGLE_CREDS_JSON:
        die("Missing GOOGLE_CREDS_JSON env var containing Service Account JSON.")
    try:
        info = json.loads(GOOGLE_CREDS_JSON)
    except Exception as e:
        die(f"GOOGLE_CREDS_JSON is not valid JSON: {e}")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
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
    sh = gc.open(SHEET_NAME)
    ws = sh.worksheet(WORKSHEET_NAME)

    api = get_alpaca()
    account = api.get_account()
    # Use 'buying_power' which is a string; convert to Decimal
    buying_power = decimal_usd(account.buying_power)

    # Pull all values in column A
    col_a = ws.col_values(1)
    if not col_a:
        print("No data found in column A; nothing to do.")
        return

    # Assume row 1 might be a header; process starting at row 2
    start_row = 2
    max_row = len(col_a)
    tickers_to_process = []
    for idx in range(start_row - 1, max_row):
        ticker = (col_a[idx] or "").strip()
        if ticker:
            tickers_to_process.append((idx + 1, ticker.upper()))

    if not tickers_to_process:
        print("No tickers listed below the header; nothing to do.")
        return

    print(f"Found {len(tickers_to_process)} ticker(s): {[t for _, t in tickers_to_process]}")

    # For each ticker, compute notional from *current* buying power each time
    # NOTE: This can overspend if orders fill instantly; Alpaca will reject if insufficient.
    # That’s acceptable for this simple bot by design.
    for row_idx, ticker in tickers_to_process:
        try:
            # Refresh buying power each iteration
            account = api.get_account()
            buying_power = decimal_usd(account.buying_power)
            notional = (buying_power * BUY_FRACTION).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if notional < MIN_NOTIONAL:
                print(f"Skipping {ticker} — notional ${notional} under minimum ${MIN_NOTIONAL}.")
                ws.update_cell(row_idx, 3, ticker)  # C
                ws.update_cell(row_idx, 4, f"SKIPPED (notional ${notional})")  # D
                continue

            order = api.submit_order(
                symbol=ticker,
                side="buy",
                type="market",
                time_in_force="day",
                notional=float(notional)
            )

            # Log immediately using the intended notional as “cost”
            ws.update_cell(row_idx, 3, ticker)              # Column C
            ws.update_cell(row_idx, 4, f"{notional}")       # Column D
            print(f"Submitted market BUY for {ticker} with notional ${notional}. Order ID: {order.id}")

        except Exception as e:
            err = f"ERROR: {e}"
            print(f"{ticker}: {err}", file=sys.stderr)
            try:
                ws.update_cell(row_idx, 3, ticker)      # Column C
                ws.update_cell(row_idx, 4, err)         # Column D
            except Exception:
                pass  # keep going for other rows

        # small pause to be nice
        time.sleep(0.4)

    # Clear column A after processing (from A2 downward)
    try:
        clear_range = f"A{start_row}:A{max_row}"
        ws.batch_clear([clear_range])
        print(f"Cleared input range {clear_range}.")
    except Exception:
        # Fallback if batch_clear not available in some contexts
        blank_block = [[""] for _ in range(max_row - start_row + 1 if max_row >= start_row else 0)]
        if blank_block:
            ws.update(f"A{start_row}:A{max_row}", blank_block)
            print(f"Cleared input range {clear_range} via update fallback.")

if __name__ == "__main__":
    main()
