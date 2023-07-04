from flask import Flask, render_template, request
import pandas as pd
import subprocess
import time

app = Flask(__name__)

@app.route('/')
def welcome():
    return render_template('welcome.html')

@app.route('/home', methods=['POST'])
def homedata():
    port = request.form.get('port')  # Get the form input value
    subprocess.Popen(['python', 'option_final.py', port])
    # Start the option_final.py script with the port value as a command-line argument
    df = pd.read_csv('data.csv')  # Read the CSV file
    underlying_values = df['underlying'].unique().tolist()  # Get unique values from 'underlying' column
    expiry_dates = df['expiry_date'].unique().tolist()  # Get unique values from 'expiry_date' column
    expiry_dates.sort()

    return render_template('index.html', underlying_values=underlying_values, expiry_dates=expiry_dates)

@app.route('/data')
@app.route('/data/<underlying>')
@app.route('/data/<underlying>/<expiry_date>')
def get_data(underlying=None, expiry_date=None):
    df = pd.read_csv('data.csv')  # Read the CSV file
    if underlying:
        if expiry_date:
            df_filtered = df[(df['underlying'] == underlying) & (df['expiry_date'] == expiry_date)]  # Filter DataFrame for the selected underlying and expiry_date
        else:
            df_filtered = df[df['underlying'] == underlying]  # Filter DataFrame for the selected underlying
    else:
        df_filtered = df  # No underlying specified, fetch all data
    
    df_filtered = df_filtered.fillna('-')

    # Calculate the change in OI
    df_filtered['Change in OI'] = round(df_filtered['Open Interest (OI)'] - df_filtered['Previous Open Interest'], 4)
    df_filtered['Change'] = round((df_filtered['Last Traded Price (LTP)'] - df_filtered['Previous Close Price']) * 100/ df_filtered['Previous Close Price'], 4)

    df_sorted = df_filtered.sort_values('option_type')  # Sort DataFrame by 'option_type' column

    return df_sorted.to_json(orient='records')

@app.route('/about')
def team():
    return render_template('team.html')

if __name__ == '__main__':
    app.run()
