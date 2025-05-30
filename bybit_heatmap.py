from flask import Flask, request, jsonify
import requests
import pandas as pd
import time

app = Flask(__name__)

EXCHANGE_APIS = {
    "binance": "https://fapi.binance.com/fapi/v1/trades?symbol=BTCUSDT&limit=500",
    "bybit": "https://api.bybit.com/v5/market/recent-trade?category=linear&symbol=BTCUSDT&limit=500",
    "okx": "https://www.okx.com/api/v5/market/trades?instId=BTC-USDT-SWAP&limit=500"
}

def fetch_trades(exchange):
    try:
        response = requests.get(EXCHANGE_APIS[exchange], timeout=5)
        if response.status_code != 200:
            print(f"{exchange} error: {response.status_code}")
            return None
        if exchange == "binance":
            df = pd.DataFrame(response.json())
            df = df[["price", "qty", "isBuyerMaker"]]
            df.columns = ["price", "size", "side"]
            df["side"] = df["side"].apply(lambda x: "sell" if x else "buy")
        elif exchange == "bybit":
            df = pd.DataFrame(response.json()["result"]["list"])
            df = df[["price", "size", "side"]]
        elif exchange == "okx":
            df = pd.DataFrame(response.json()["data"])
            df = df[["px", "sz", "side"]]
            df.columns = ["price", "size", "side"]
        df["price"] = df["price"].astype(float)
        df["size"] = df["size"].astype(float)
        return df
    except Exception as e:
        print(f"{exchange} error: {e}")
        return None

def merge_liquidations():
    all_trades = [fetch_trades(ex) for ex in EXCHANGE_APIS]
    all_trades = [df for df in all_trades if df is not None]
    if not all_trades:
        return None
    merged_df = pd.concat(all_trades, ignore_index=True)
    liquidations = merged_df[merged_df["size"] > 0.1]
    if liquidations.empty:
        return None
    liquidations["price_bin"] = (liquidations["price"] // 100) * 100
    heatmap = liquidations.groupby(["price_bin", "side"])["size"].sum().unstack(fill_value=0)
    top_long = heatmap["buy"].idxmax() if "buy" in heatmap else None
    top_short = heatmap["sell"].idxmax() if "sell" in heatmap else None
    return {
        "long_liq": float(top_long) if top_long else 0.0,
        "short_liq": float(top_short) if top_short else 0.0
    }

@app.route('/webhook', methods=['POST'])
def webhook():
    data = merge_liquidations()
    if data is None:
        return jsonify({"error": "No data"}), 500
    print(f"Webhook: {data}")
    return jsonify(data), 200

if __name__ == '__main__':
    while True:
        data = merge_liquidations()
        if data:
            print(f"Top Zones: {data}")
        time.sleep(300)