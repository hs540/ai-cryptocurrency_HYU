import requests
import csv
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s',
                    handlers=[
                        logging.FileHandler("upbit_log.txt"),
                        logging.StreamHandler()
                    ])

run_for_seconds = timedelta(seconds=172800)  # 48 hours in seconds
#run_for_seconds = timedelta(seconds=60)

start_time = datetime.now()
url = "https://api.upbit.com/v1/orderbook"
headers = {"accept": "application/json"}
params = {'markets': 'KRW-BTC'}


timestamp_str = datetime.now().strftime('%Y-%m-%d-%H%M')
file_name = f"{timestamp_str}-upbit-btc-orderbook.csv"

logging.info(f"Starting data collection for {run_for_seconds.total_seconds()} seconds.")

last_update_time = datetime.min 

while True:
    timestamp = datetime.now()
    elapsed_time = timestamp - start_time

    if elapsed_time >= run_for_seconds:
        logging.info("Data collection complete.") 
        break


    if (timestamp - last_update_time).total_seconds() < 1.0:
        continue

    last_update_time = timestamp
    try:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            orderbook_data = response.json()

            with open(file_name, mode='a', newline='') as file:
                writer = csv.writer(file)

                file_empty = file.tell() == 0
                if file_empty:
                    writer.writerow(['price', 'quantity', 'type', 'timestamp'])

                for market_data in orderbook_data:
                    market_timestamp = datetime.fromtimestamp(market_data['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')

                    level_5_data = market_data['orderbook_units'][5:10]
                    level_5_bids = [(unit['bid_price'], unit['bid_size'], '0', market_timestamp) for unit in level_5_data]
                    level_5_asks = [(unit['ask_price'], unit['ask_size'], '1', market_timestamp) for unit in level_5_data]

                    for bid in level_5_bids:
                        writer.writerow(bid)
                    for ask in level_5_asks:
                        writer.writerow(ask)

            logging.info("Successfully wrote level 5 data to CSV.")
        else:
            logging.error(f"Failed to fetch data: {response.status_code} {response.text}")
            continue

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        continue

print("Script has finished executing.")
