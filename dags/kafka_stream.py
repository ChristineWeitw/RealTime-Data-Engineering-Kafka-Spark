# Description: This script is used to get the stock price of the given stock symbol.
from yahoo_fin import stock_info as si
from datetime import datetime

# Get the stock price of the given stock symbol
def get_stock_price(symbol):
    try:
        price = si.get_live_price(symbol)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return price, timestamp
    except Exception as e:
        print(f"An error occurred: {e}")
        return None, None

# Main function
def main():
    format_data = {}
    symbols = ["0050.TW", "VOO", "NVDA"]
    for symbol in symbols:
        price, timestamp = get_stock_price(symbol)
        format_data[symbol] = [price, timestamp]
        
    print('final format_data:/n', format_data)
    return format_data

if __name__ == "__main__":
    main()




