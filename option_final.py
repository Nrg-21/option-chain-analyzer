import socket
import struct
import re
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import norm
from scipy.optimize import brentq
from scipy.stats import norm
from scipy.optimize import newton
from math import log, sqrt, exp
import pandas as pd
import math
import sys

try:
    port_in = sys.argv[1]
except:
    port_in = 4000

# Define the field structure
fields = [
    ("Packet Length", "I", 0, 4),
    ("Trading Symbol", "30s", 4, 30),
    ("Sequence Number", "q", 34, 8),
    ("Timestamp", "q", 42, 8),
    ("Last Traded Price (LTP)", "q", 50, 8),
    ("Last Traded Quantity", "q", 58, 8),
    ("Volume", "q", 66, 8),
    ("Bid Price", "q", 74, 8),
    ("Bid Quantity", "q", 82, 8),
    ("Ask Price", "q", 90, 8),
    ("Ask Quantity", "q", 98, 8),
    ("Open Interest (OI)", "q", 106, 8),
    ("Previous Close Price", "q", 114, 8),
    ("Previous Open Interest", "q", 122, 8)
]

N_prime = norm.pdf
N = norm.cdf

def calculate_implied_volatility(S, K, r, T, option_price, option_type):
    """
    Calculates the implied volatility of an American option using the Brent method as an alternative to Newton-Raphson.
    Args:
        S (float): Current price of the underlying asset
        K (float): Strike price of the option
        r (float): Risk-free interest rate
        T (float): Time to expiration in years
        option_price (float): Observed market price of the option
        option_type (str): Option type, either 'call' or 'put'
    Returns:
        float: Implied volatility of the option
    """
    
    def black_scholes_price(volatility):
        # Option pricing calculations using the Black-Scholes model
        d1 = (math.log(S / K) + (r + 0.5 * volatility**2) * T) / (volatility * math.sqrt(T))
        d2 = d1 - volatility * math.sqrt(T)
        
        if option_type == 'CE':
            price = S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
        else:
            price = K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        
        return price
    
    def implied_volatility_function(volatility):
        # Function to find the difference between the observed and calculated option prices
        return black_scholes_price(volatility) - option_price
    
    implied_volatility = brentq(implied_volatility_function, a=0.01, b=10.0)
    return implied_volatility

all_ltp = 43982.5
financial_ltp = 19403.6
midcap_ltp = 7856.5
main_ltp = 18548.8

port = int(port_in)
# Create a socket object
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Send the data packet to the server
sock.connect(('127.0.0.1', port))
sock.send("Hi Little Endian this is a send packect to establish our connection stream!!!".encode())

# Create an empty DataFrame
columns = ["Last Traded Price (LTP)", "Last Traded Quantity", "Volume", "Bid Price", "Bid Quantity", "Ask Price","Ask Quantity", "Open Interest (OI)", "Previous Close Price", "Previous Open Interest", "expiry_date","strike_price", "option_type", "underlying", "implied volatility","In the money"]
data = pd.DataFrame(columns=columns)
csv_file = "data.csv"


while True:
    # Receive the packet length
    packet_length_data = sock.recv(4)
    packet_length = 130

    # Receive the complete packet
    packet_data = packet_length_data + sock.recv(packet_length - 4)

    # Process the packet and extract fields
    field_values = {}
    for field in fields:

        field_name, data_type, offset, length = field
        field_data = struct.unpack(data_type, packet_data[offset:offset + length])[0]

        # Decode the field data if it's a string
        if isinstance(field_data, bytes):
            field_data = field_data.decode().rstrip('\x00')

        # Handle special parsing for Trading Symbol
        if field_name == "Trading Symbol":
            pattern = r'^([A-Za-z]+)(\d{2}[A-Z]{3}\d{2})?(\d+(?:\.\d+)?)?([A-Z]+)?$'
            match = re.match(pattern, field_data)
            if match:
                groups = match.groups()
                underlying = groups[0]
                expiry_date = groups[1]
                K = float(groups[2]) if groups[2] else None
                option_type = groups[3]
                field_data = {
                    "underlying": underlying,
                    "expiry_date": expiry_date,
                    "strike_price": K,
                    "option_type": option_type
                }
                # Extract expiry day and time
                if expiry_date:
                    expiry_day = datetime.strptime(expiry_date, "%d%b%y").date()
                    expiry_time = datetime.combine(expiry_day, datetime.min.time()).replace(hour=15, minute=30,second=0)
                    field_data["expiry_time"] = expiry_time
                else:
                    field_data["expiry_time"] = None

        elif field_name == "Timestamp":
            timestamp_ms = field_data
            timestamp_s = timestamp_ms / 1000
            field_data = datetime.fromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')
        elif field_name in ["Last Traded Price (LTP)", "Bid Price", "Ask Price", "Previous Close Price"]:
            field_data /= 100
        field_values[field_name] = field_data

    # Extract the required variables
    trading_symbol = field_values["Trading Symbol"]
    C = field_values["Last Traded Price (LTP)"]
    if field_values["Trading Symbol"]["underlying"]=="ALLBANKS":
        S = all_ltp
    if field_values["Trading Symbol"]["underlying"]=="FINANCIALS":
        S = financial_ltp
    if field_values["Trading Symbol"]["underlying"]=="MIDCAPS":
        S = midcap_ltp
    if field_values["Trading Symbol"]["underlying"]=="MAINIDX":
        S = main_ltp
    K = field_values["Trading Symbol"]["strike_price"]
    r = 0.05
    T = field_values["Trading Symbol"]["expiry_time"]

    flag=field_values["Trading Symbol"]["option_type"]

    # Calculate implied volatility
    current_time = datetime.now()
    TTM_days = (expiry_time - current_time).total_seconds() / 60.0 / 1440.0  # Calculate time to maturity in days
    TTM= TTM_days/ 365.25 
    if K != None:
        try:
            iv = round(calculate_implied_volatility(S, K, r, TTM, C, flag) * 100, 2)
        except:
            iv = 0
            

    else:
        continue

    if K is None and expiry_date is None and option_type is None:
        if underlying == "ALLBANKS":
            all_ltp = S

        elif underlying == "FINANCIALS":
            financial_ltp = S

        elif underlying == "MIDCAPS":
            midcap_ltp = S

        elif underlying == "MAINIDX":
            main_ltp = S

    # Determine if the option is in the money or out of the money
    if field_values["Trading Symbol"]["option_type"] == "CE":
        in_the_money = S > K
    elif field_values["Trading Symbol"]["option_type"] == "PE":
        in_the_money = S < K
    else:
        in_the_money = False

    # Add the data to the DataFrame
    existing_row = data[(data["underlying"] == underlying) & (data["option_type"] == option_type) & (data["strike_price"] == K) & (data["expiry_date"] == expiry_date)]
    if not existing_row.empty:
        # Update the existing row with the new data
        existing_row_index = existing_row.index[0]
        data.loc[existing_row_index, "Last Traded Price (LTP)"] = C
        data.loc[existing_row_index, "Last Traded Quantity"] = field_values["Last Traded Quantity"]
        data.loc[existing_row_index, "Volume"] = field_values["Volume"]
        data.loc[existing_row_index, "Bid Price"] = field_values["Bid Price"]
        data.loc[existing_row_index, "Bid Quantity"] = field_values["Bid Quantity"]
        data.loc[existing_row_index, "Ask Price"] = field_values["Ask Price"]
        data.loc[existing_row_index, "Ask Quantity"] = field_values["Ask Quantity"]
        data.loc[existing_row_index, "Open Interest (OI)"] = field_values["Open Interest (OI)"]
        data.loc[existing_row_index, "Previous Close Price"] = field_values["Previous Close Price"]
        data.loc[existing_row_index, "Previous Open Interest"] = field_values["Previous Open Interest"]
        data.loc[existing_row_index, "implied volatility"] = iv if iv else None
        data.loc[existing_row_index, "In the money"] = in_the_money
    else:
        # Append a new row to the DataFrame
        data = data._append({
            "underlying": underlying,
            "option_type": option_type,
            "strike_price": K,
            "expiry_date": expiry_date,
            "Last Traded Price (LTP)": C,
            "Last Traded Quantity": field_values["Last Traded Quantity"],
            "Volume": field_values["Volume"],
            "Bid Price": field_values["Bid Price"],
            "Bid Quantity": field_values["Bid Quantity"],
            "Ask Price": field_values["Ask Price"],
            "Ask Quantity": field_values["Ask Quantity"],
            "Open Interest (OI)": field_values["Open Interest (OI)"],
            "Previous Close Price": field_values["Previous Close Price"],
            "Previous Open Interest": field_values["Previous Open Interest"],
            "implied volatility": iv if iv else None,
            "In the money": in_the_money,
        }, ignore_index=True)
    data.to_csv(csv_file, index=False)

# Close the socket connection
sock.close()