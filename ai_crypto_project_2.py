import pandas as pd
import os

input_file_name = '2023-11-09-1919-upbit-btc-orderbook.csv'

# Load the dataset
print("Loading dataset...")
df = pd.read_csv(input_file_name, header=None, skiprows=1)
df.columns = ['price', 'quantity', 'type', 'timestamp']
print("Dataset loaded successfully.")

# Constants
ratio = 0.2
level = 5
interval = 1

# Function to process each group of rows with the same timestamp
def process_group(group):
    print(f"Processing group for timestamp: {group['timestamp'].iloc[0]}")  # Print current timestamp

    # Separate bids and asks
    bids = group[group['type'] == 0]
    asks = group[group['type'] == 1]

    # Calculate mid_price
    if not bids.empty and not asks.empty:
        top_bid_price = bids['price'].max()
        top_ask_price = asks['price'].min()
        mid_price = (top_bid_price + top_ask_price) * 0.5
    else:
        mid_price = None

    # Compute quantities and prices for bids and asks
    bids['weighted_qty'] = bids['quantity'] ** ratio
    asks['weighted_qty'] = asks['quantity'] ** ratio
    bids['weighted_price'] = bids['price'] * bids['weighted_qty']
    asks['weighted_price'] = asks['price'] * asks['weighted_qty']

    # Sums for bids and asks
    bidQty = bids['weighted_qty'].sum()
    askQty = asks['weighted_qty'].sum()
    bidPx = bids['weighted_price'].sum()
    askPx = asks['weighted_price'].sum()

    # Calculate book_price and book_imbalance
    if bidQty and askQty:
        book_price = (((askQty * bidPx) / bidQty) + ((bidQty * askPx) / askQty))/(bidQty + askQty)
        book_imbalance = (book_price - mid_price) / interval
    else:
        book_price = None
        book_imbalance = None

    return pd.Series([mid_price, book_imbalance], index=['mid_price', 'book_imbalance'])

# Group by timestamp and apply the function
print("Starting data processing...")
grouped = df.groupby('timestamp')
processed_df = grouped.apply(process_group)
print("Data processing complete.")

# Add timestamp back to the dataframe
processed_df['timestamp'] = processed_df.index

# Create new filename
base_name, ext = os.path.splitext(input_file_name)
new_file_name = base_name.replace('upbit-btc-orderbook', 'exchange-market-feature') + ext

print(f"Saving results to {new_file_name}...")
processed_df.to_csv(new_file_name, index=False)
print(f"Results saved to {new_file_name}.")
