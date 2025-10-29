import os, json, time, math, sys
from typing import List
from decimal import Decimal, ROUND_DOWN
from tenacity import retry, wait_fixed, stop_after_attempt

# -------- Alpaca --------
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
from alpaca.trading.requests import (
    MarketOrderRequest,
    TakeProfitRequest,
    StopLossRequest,
)
from alpaca.trading.models import Position
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
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.03"))      # 3% stop
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))  # +5% TP

# ---- helpers
def dquant6(x: float) -> str:
    """Quantize to 6 decimal places as string for fractional qty."""
    return str(Decimal(x).quantize(Decimal("0.000001"), rounding=ROUND_DOWN))

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

def get_position_qty(trading: TradingClient, symbol: str) -> float:
    try:
        p: Position = trading.get_open_position(symbol)
        return float(p.qty)
    except Exception:
        return 0.0

@retry(wait=wait_fixed(1), stop=stop_after_attempt(15))
def wait_for_order_accept_by_id(trading: TradingClient, order_id: str):
    """Poll order by ID until it is accepted/new/filled/partially_filled."""
    order = trading.get_order_by_id(order_id)
    if order.status in ("new", "accepted", "partially_filled", "filled", "done_for_day"):
        return order
    raise RuntimeError(f"Order {order_id} not yet accepted (status={order.status})")

def validate_alpaca_or_exit(trading: TradingClient):
    try:
        acct = trading.get_account()
        print(f"Alpaca auth OK. Account: {acct.account_number} | paper={ALPACA_PAPER}")
        return True
    except Exception as e:
        print("ERROR: Alpaca authentication failed.")
        print(f"Detail: {e}")
        print("Quick checks:")
        print("  1) Ensure ALPACA_KEY_ID and ALPACA_SECRET_KEY are set correctly in Railway (no quotes/spaces).")
        print("  2) If PAPER keys, set ALPACA_PAPER=true. If LIVE keys, set ALPACA_PAPER=false.")
        print("  3) Enable fractional trading if using notional buys.")
        print("Aborting without changing your sheet.")
        return False

def place_fractional_buy(trading: TradingClient, symbol: str, notional: float) -> str:
    req = MarketOrderRequest(
        symbol=symbol,
        notional=round(max(notional, 1.00), 2),  # fractional $ buy, min $1
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
    )
    order = trading.submit_order(req)
    return str(order.id)

def place_oco_sell(trading: TradingClient, symbol: str, qty: float, tp_price: float, sl_price: float):
    # OCO requires qty, not notional; supports fractional qty for equities
    qty_str = dquant6(qty)
    req = MarketOrderRequest(  # container for OCO legs
        symbol=symbol,
        qty=qty_str,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        order_class=OrderClass.OCO,
        take_profit=TakeProfitRequest(limit_price=round(tp_price, 2)),
        stop_loss=StopLossRequest(stop_price=round(sl_price, 2)),
    )
    trading.submit_order(req)

def main():
    # ---- Clients
    trading = TradingClient(ALPACA_KEY, ALPACA_SECRET, paper=ALPACA_PAPER)
    if not validate_alpaca_or_exit(trading):
        sys.exit(1)

    data_client = StockHistoricalDataClient(ALPACA_KEY, ALPACA_SECRET)
    gc = get_gspread_client()

    # ---- Read tickers
    tickers = read_tickers(gc)
    if not tickers:
        print("No tickers found in column A; exiting.")
        return

    placed_ocos = []
    for symbol in tickers:
        try:
            account = trading.get_account()
            buying_power = float(account.buying_power)
            alloc = buying_power * BUY_PERCENT
            if alloc < 1.0:
                print(f"Skipping {symbol}: allocation ${alloc:.2f} < $1 minimum.")
                continue

            # Pre-buy position qty to compute delta
            qty_before = get_position_qty(trading, symbol)

            # 1) Fractional buy (simple order)
            buy_order_id = place_fractional_buy(trading, symbol, alloc)
            wait_for_order_accept_by_id(trading, buy_order_id)

            # 2) Poll for fill & delta qty
            qty_after = qty_before
            for _ in range(60):  # up to ~60s
                time.sleep(1)
                qty_after = get_position_qty(trading, symbol)
                if qty_after > qty_before:
                    break

            delta_qty = max(0.0, qty_after - qty_before)
            if delta_qty <= 0.0:
                print(f"Warning: {symbol} fractional buy not reflected in position yet; skipping OCO.")
                continue

            # Reference price for exits (latest)
            px = get_latest_price(data_client, symbol)
            tp_price = px * (1.0 + TAKE_PROFIT_PCT)
            sl_price = px * (1.0 - STOP_LOSS_PCT)

            # 3) OCO for the delta qty (GTC)
            place_oco_sell(trading, symbol, delta_qty, tp_price, sl_price)
            print(f"Placed OCO for {symbol}: qty={dquant6(delta_qty)}, tp=~{tp_price:.2f}, sl=~{sl_price:.2f}")
            placed_ocos.append(symbol)

            time.sleep(0.5)

        except Exception as e:
            print(f"Error processing {symbol}: {e}")

    # ---- Clear Column A only if we placed at least one OCO
    if placed_ocos:
        try:
            clear_column_a(gc)
            print("Cleared Column A in 'Alpaca Integration'.")
        except Exception as e:
            print(f"Warning: failed to clear Column A — {e}")
    else:
        print("No successful OCOs placed; Column A left unchanged.")

    print(f"Done. Placed OCOs for: {placed_ocos}")

if __name__ == "__main__":
    main()
