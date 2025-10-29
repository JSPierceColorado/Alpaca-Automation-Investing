import os, json, time, math
from typing import List
from tenacity import retry, wait_fixed, stop_after_attempt

# -------- Alpaca --------
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
from alpaca.trading.requests import MarketOrderRequest
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest

# -------- Google Sheets --------
import gspread
from google.oauth2.service_account import Credentials

SHEET_NAME = os.getenv("SHEET_NAME", "Active-Investing")
TAB_NAME = os.getenv("TAB_NAME", "Alpaca Integration")

ALPACA_KEY = os.environ["ALPACA_KEY_ID"]
ALPACA_SECRET = os.environ["ALPACA_SECRET_KEY"]
# If you use paper trading, set ALPACA_PAPER=true in Railway
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "true").lower() in ("1","true","yes")
# Optional: override base URL if needed
ALPACA_BASE_URL = os.getenv("ALPACA_BASE_URL")  # usually not needed with paper=True

GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"]

BUY_PERCENT = float(os.getenv("BUY_PERCENT", "0.07"))   # 7% per ticker
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.03"))  # 3%
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))  # +5%

# -------- Helpers --------
def get_gspread_client():
    info = json.loads(GOOGLE_CREDS_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def read_tickers(gc) -> List[str]:
    sh = gc.open(SHEET_NAME)
    ws = sh.worksheet(TAB_NAME)
    col = ws.col_values(1)  # Column A
    # Strip header if present and remove blanks
    tickers = [t.strip().upper() for t in col if t and t.strip()]
    return tickers

def clear_column_a(gc):
    sh = gc.open(SHEET_NAME)
    ws = sh.worksheet(TAB_NAME)
    ws.batch_clear(["A:A"])

def get_latest_price(client: StockHistoricalDataClient, symbol: str) -> float:
    """
    Use the snapshot endpoint to grab the latest trade/quote; fallback to last minute bar if needed.
    """
    req = StockSnapshotRequest(symbol_or_symbols=symbol)
    snap = client.get_stock_snapshot(req)
    s = snap[symbol]
    # Prefer latest trade price; fall back to latest quote mid or last minute bar close
    if s.latest_trade:
        return float(s.latest_trade.price)
    if s.latest_quote:
        return float((s.latest_quote.ask_price + s.latest_quote.bid_price) / 2.0)
    if s.latest_minute_bar:
        return float(s.latest_minute_bar.close)
    raise RuntimeError(f"Could not get a current price for {symbol}")

@retry(wait=wait_fixed(1), stop=stop_after_attempt(10))
def wait_for_parent_accept(trading: TradingClient, client_order_id: str):
    # lightweight poll so we can at least observe acceptance before exiting
    order = trading.get_order_by_client_order_id(client_order_id)
    if order.status in ("new","accepted","partially_filled","filled","done_for_day"):
        return order
    raise RuntimeError("Order not yet accepted")

def main():
    # ---- Clients
    trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=ALPACA_PAPER, url=ALPACA_BASE_URL)
    data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    gc = get_gspread_client()

    # ---- Read tickers
    tickers = read_tickers(gc)
    if not tickers:
        print("No tickers found in column A; exiting.")
        return

    # ---- Iterate tickers and place bracket orders
    placed = []
    for symbol in tickers:
        try:
            account = trading.get_account()
            buying_power = float(account.buying_power)
            alloc = buying_power * BUY_PERCENT
            if alloc < 1.0:
                print(f"Skipping {symbol}: allocation ${alloc:.2f} < $1 minimum.")
                continue

            # Compute qty using latest price; use whole shares for bracket reliability
            px = get_latest_price(data_client, symbol)
            qty = math.floor(alloc / px)
            if qty < 1:
                print(f"Skipping {symbol}: allocation ${alloc:.2f} insufficient for 1 share at ~${px:.2f}.")
                continue

            # Target exits from reference price
            tp_price = round(px * (1.0 + TAKE_PROFIT_PCT), 2)
            sl_price = round(px * (1.0 - STOP_LOSS_PCT), 2)

            client_order_id = f"buy_{symbol}_{int(time.time())}"

            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                order_class=OrderClass.BRACKET,
                take_profit={"limit_price": tp_price},
                stop_loss={"stop_price": sl_price},
                client_order_id=client_order_id,
            )

            order = trading.submit_order(req)
            # Optional: quick poll to ensure the parent is accepted before moving on
            wait_for_parent_accept(trading, client_order_id)
            print(f"Placed BRACKET for {symbol}: qty={qty}, tp={tp_price}, sl={sl_price}")
            placed.append(symbol)

            # Be polite to rate limits
            time.sleep(0.5)

        except Exception as e:
            print(f"Error placing order for {symbol}: {e}")

    # ---- Clear Column A regardless of per-symbol success
    try:
        clear_column_a(gc)
        print("Cleared Column A in 'Alpaca Integration'.")
    except Exception as e:
        print(f"Warning: failed to clear Column A — {e}")

    print(f"Done. Placed orders for: {placed}")

if __name__ == "__main__":
    main()
