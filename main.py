import os, json, time, math, sys, traceback
from typing import List
from decimal import Decimal, ROUND_DOWN
from tenacity import retry, wait_fixed, stop_after_attempt

# -------- Alpaca --------
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass, OrderType
from alpaca.trading.requests import (
    MarketOrderRequest,
    TakeProfitRequest,
    StopLossRequest,
    OrderRequest,  # used for OCO (limit-type container)
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

# ---- Strategy params
BUY_PERCENT = float(os.getenv("BUY_PERCENT", "0.07"))            # 7% baseline per ticker
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "0.03"))        # -3% stop
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.05"))    # +5% TP
MIN_NOTIONAL = float(os.getenv("MIN_NOTIONAL", "1.00"))          # $1 min for fractional notional (Alpaca)
SPEND_CAP = float(os.getenv("SPEND_CAP", "0.90"))                # use up to 90% of buying power this run

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
        print("Aborting without changing your sheet.")
        return False

def place_fractional_buy(trading: TradingClient, symbol: str, notional: float) -> str:
    amt = round(max(notional, MIN_NOTIONAL), 2)
    req = MarketOrderRequest(
        symbol=symbol,
        notional=amt,  # fractional $ buy
        side=OrderSide.BUY,
        time_in_force=TimeInForce.DAY,
    )
    order = trading.submit_order(req)
    print(f"[BUY] {symbol}: notional=${amt:.2f} -> order_id={order.id}")
    return str(order.id)

def place_oco_sell(trading: TradingClient, symbol: str, qty: float, tp_price: float, sl_price: float):
    """
    Submit OCO (GTC) for fractional qty. Alpaca requires OCOs to be limit-type.
    """
    qty_str = dquant6(qty)
    req = OrderRequest(
        symbol=symbol,
        qty=qty_str,
        side=OrderSide.SELL,
        time_in_force=TimeInForce.GTC,
        order_class=OrderClass.OCO,
        type=OrderType.LIMIT,  # REQUIRED: OCO must be limit-type container
        take_profit=TakeProfitRequest(limit_price=round(tp_price, 2)),
        stop_loss=StopLossRequest(stop_price=round(sl_price, 2)),
    )
    order = trading.submit_order(req)
    print(f"[OCO] {symbol}: qty={qty_str} tp={tp_price:.2f} sl={sl_price:.2f} -> order_id={order.id}")

def compute_per_ticker_alloc(buying_power: float, n_tickers: int) -> float:
    """
    Compute a per-ticker dollar notional that:
      - starts from BUY_PERCENT * buying_power
      - is at least MIN_NOTIONAL
      - is scaled down so total spend <= SPEND_CAP * buying_power
    """
    if n_tickers <= 0 or buying_power <= 0:
        return 0.0
    cap = max(0.0, SPEND_CAP) * buying_power
    baseline = max(MIN_NOTIONAL, BUY_PERCENT * buying_power)
    per = baseline
    if per * n_tickers > cap and n_tickers > 0:
        per = max(MIN_NOTIONAL, math.floor((cap / n_tickers) * 100) / 100.0)  # floor to cents
    return per

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

    # ---- Budget planning
    account = trading.get_account()
    buying_power = float(account.buying_power)
    n = len(tickers)
    per_alloc = compute_per_ticker_alloc(buying_power, n)
    total_planned = per_alloc * n
    cap_amount = SPEND_CAP * buying_power

    print(f"[BUDGET] buying_power=${buying_power:.2f} | tickers={n} | BUY_PERCENT={BUY_PERCENT:.4f}")
    print(f"[BUDGET] MIN_NOTIONAL=${MIN_NOTIONAL:.2f} | SPEND_CAP={SPEND_CAP:.2f} -> cap_amount=${cap_amount:.2f}")
    print(f"[BUDGET] per_ticker=${per_alloc:.2f} -> total_planned=${total_planned:.2f}")

    if per_alloc < MIN_NOTIONAL:
        print(f"[BUDGET] per_ticker ${per_alloc:.2f} < MIN_NOTIONAL ${MIN_NOTIONAL:.2f}; nothing to do.")
        return
    if total_planned < MIN_NOTIONAL:
        print(f"[BUDGET] total_planned ${total_planned:.2f} < $1; nothing to do.")
        return

    placed_ocos = []
    remaining_cap = cap_amount
    for symbol in tickers:
        try:
            # Skip if remaining budget would drop below $1
            alloc = round(min(per_alloc, remaining_cap), 2)
            print(f"[ALLOC] {symbol}: remaining_cap=${remaining_cap:.2f} -> alloc=${alloc:.2f}")
            if alloc < MIN_NOTIONAL:
                print(f"Skipping {symbol}: alloc ${alloc:.2f} < MIN_NOTIONAL ${MIN_NOTIONAL:.2f}")
                continue

            # Pre-buy position qty to compute delta
            qty_before = get_position_qty(trading, symbol)
            print(f"[POS] {symbol}: qty_before={qty_before}")

            # 1) Fractional buy (simple)
            buy_order_id = place_fractional_buy(trading, symbol, alloc)
            wait_for_order_accept_by_id(trading, buy_order_id)

            # 2) Poll for fill & delta qty
            qty_after = qty_before
            for i in range(60):  # up to ~60s
                time.sleep(1)
                qty_after = get_position_qty(trading, symbol)
                print(f"[POLL] {symbol}: i={i} qty_after={qty_after}")
                if qty_after > qty_before:
                    break

            delta_qty = max(0.0, qty_after - qty_before)
            print(f"[DELTA] {symbol}: delta_qty={delta_qty}")
            if delta_qty <= 0.0:
                print(f"Warning: {symbol} fractional buy not reflected in position yet; skipping OCO.")
                remaining_cap = max(0.0, remaining_cap - alloc)
                continue

            # Latest price and exits
            px = get_latest_price(data_client, symbol)
            tp_price = px * (1.0 + TAKE_PROFIT_PCT)
            sl_price = px * (1.0 - STOP_LOSS_PCT)
            print(f"[PX] {symbol}: latest={px:.4f} -> TP={tp_price:.4f} SL={sl_price:.4f}")

            # 3) OCO for the delta qty (GTC)
            place_oco_sell(trading, symbol, delta_qty, tp_price, sl_price)
            placed_ocos.append(symbol)

            remaining_cap = max(0.0, remaining_cap - alloc)
            print(f"[CAP] {symbol}: remaining_cap now ${remaining_cap:.2f}")
            time.sleep(0.5)

        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")
            traceback.print_exc()

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
