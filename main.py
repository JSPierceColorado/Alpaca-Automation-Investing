import os, json, time, math, sys
from typing import List
from tenacity import retry, wait_fixed, stop_after_attempt

# -------- Alpaca --------
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
from alpaca.trading.requests import (
    MarketOrderRequest,
    TakeProfitRequest,
    StopLossRequest,
)
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest

# -------- Google Sheets --------
import gspread
from google.oauth2.service_account import Credentials

SHEET_NAME = os.getenv("SHEET_NAME", "Active-Investing")
TAB_NAME = os.getenv("TAB_NAME", "Alpaca Integration")

ALPACA_KEY = os.environ.get("ALPACA_KEY_ID", "").strip()
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "").strip()
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "true").lower().strip() in ("1", "true", "yes")

GOOGLE_CREDS_JSON = os.environ["GOOGLE_CREDS_JSON"]

BUY_PERCENT = float(os.getenv("BUY_PERCENT", "0.07"))          # 7% per ticker
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.03"))      # 3%
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))  # +5%

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
    tickers = [t.strip().upper() for t in col if t and t.strip()]
    # Drop common header words if present
    if tickers and tickers[0] in ("TICKER", "TICKERS", "SYMBOL", "SYMBOLS"):
        tickers = tickers[1:]
    return tickers

def clear_column_a(gc):
    sh = gc.open(SHEET_NAME)
    ws = sh.worksheet(TAB_NAME)
    ws.batch_clear(["A:A"])

def get_latest_price(client: StockHistoricalDataClient, symbol: str) -> float:
    req = StockSnapshotRequest(symbol_or_symbols=symbol)
    snap = client.get_stock_snapshot(req)
    s = snap[symbol]
    if getattr(s, "latest_trade", None):
        return float(s.latest_trade.price)
    if getattr(s, "latest_quote", None):
        return float((s.latest_quote.ask_price + s.latest_quote.bid_price) / 2.0)
    if getattr(s, "latest_minute_bar", None):
        return float(s.latest_minute_bar.close)
    raise RuntimeError(f"Could not get a current price for {symbol}")

@retry(wait=wait_fixed(1), stop=stop_after_attempt(10))
def wait_for_parent_accept(trading: TradingClient, client_order_id: str):
    order = trading.get_order_by_client_order_id(client_order_id)
    if order.status in ("new", "accepted", "partially_filled", "filled", "done_for_day"):
        return order
    raise RuntimeError("Order not yet accepted")

def validate_alpaca_or_exit(trading: TradingClient):
    """
    Validate credentials & environment before we touch the sheet.
    If unauthorized, print helpful hints and exit with nonzero code.
    """
    try:
        acct = trading.get_account()
        print(f"Alpaca auth OK. Account: {acct.account_number} | paper={ALPACA_PAPER}")
        return True
    except Exception as e:
        msg = str(e)
        print("ERROR: Alpaca authentication failed.")
        print(f"Detail: {msg}")
        print("Quick checks:")
        print("  1) Ensure ALPACA_KEY_ID and ALPACA_SECRET_KEY are set correctly in Railway (no quotes/spaces).")
        print("  2) If these are PAPER keys, set ALPACA_PAPER=true. If LIVE keys, set ALPACA_PAPER=false.")
        print("  3) If LIVE: confirm your live account is approved/enabled for trading.")
        print("Aborting without changing your sheet.")
        return False

def main():
    # ---- Clients
    trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=ALPACA_PAPER)
    # Validate auth BEFORE reading/clearing sheet
    if not validate_alpaca_or_exit(trading):
        sys.exit(1)

    data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    gc = get_gspread_client()

    # ---- Read tickers
    tickers = read_tickers(gc)
    if not tickers:
        print("No tickers found in column A; exiting.")
        return

    placed = []
    for symbol in tickers:
        try:
            account = trading.get_account()
            buying_power = float(account.buying_power)
            alloc = buying_power * BUY_PERCENT
            if alloc < 1.0:
                print(f"Skipping {symbol}: allocation ${alloc:.2f} < $1 minimum.")
                continue

            px = get_latest_price(data_client, symbol)
            qty = math.floor(alloc / px)
            if qty < 1:
                print(f"Skipping {symbol}: allocation ${alloc:.2f} insufficient for 1 share at ~${px:.2f}.")
                continue

            tp_price = round(px * (1.0 + TAKE_PROFIT_PCT), 2)
            sl_price = round(px * (1.0 - STOP_LOSS_PCT), 2)
            client_order_id = f"buy_{symbol}_{int(time.time())}"

            req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
                order_class=OrderClass.BRACKET,
                take_profit=TakeProfitRequest(limit_price=tp_price),
                stop_loss=StopLossRequest(stop_price=sl_price),
                client_order_id=client_order_id,
            )

            order = trading.submit_order(req)
            wait_for_parent_accept(trading, client_order_id)
            print(f"Placed BRACKET for {symbol}: qty={qty}, tp={tp_price}, sl={sl_price}")
            placed.append(symbol)
            time.sleep(0.5)

        except Exception as e:
            print(f"Error placing order for {symbol}: {e}")

    # ---- Only clear Column A if we actually placed at least one order
    if placed:
        try:
            clear_column_a(gc)
            print("Cleared Column A in 'Alpaca Integration'.")
        except Exception as e:
            print(f"Warning: failed to clear Column A — {e}")
    else:
        print("No successful orders placed; Column A left unchanged.")

    print(f"Done. Placed orders for: {placed}")

if __name__ == "__main__":
    main()
