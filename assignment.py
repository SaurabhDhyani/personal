import requests
import sqlite3
import pandas as pd
import os

def get_conversion_factor():
    # retrieve the USD to INR conversion rate from a free API
    api_key = '14cff8b00ae64a29985a670e855a6273'
    url = f'https://api.currencyfreaks.com/latest?apikey={api_key}&symbols=INR'
    response = requests.get(url)
    data = response.json()
    factor = data['rates']['INR']
    return float(factor)

# read the database credentials from a configuration file
config_file = os.path.expanduser('E:/DN/assignment/.config/india_gdp.cfg')
with open(config_file) as f:
    config = {}
    for line in f:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        try:
            key, value = line.split('=', 1)
        except ValueError:
            print(f"Warning: invalid line in config file: {line}")
            continue
        config[key.strip()] = value.strip()

    db_file = config['db_file']


# connect to the database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# create the table if it doesn't exist
create_table_query = """
    CREATE TABLE IF NOT EXISTS india_gdp (
        year INTEGER PRIMARY KEY,
        gdp_usd REAL NOT NULL,
        gdp_inr REAL NOT NULL
    );
"""
cursor.execute(create_table_query)

# retrieve the GDP data from the source URL
response = requests.get('https://www.statista.com/statistics/263771/gross-domestic-product-gdp-in-india/')
response.raise_for_status()

# parse the HTML table to extract the GDP data
tables = pd.read_html(response.text)
df = tables[0]

# convert the data types and rename the columns
df = df.rename(columns={'Characteristic': 'year', 'GDP in billion U.S. dollars': 'gdp_usd'})
df['year'] = df['year'].str.replace('*', '').astype(int)
df['gdp_inr'] = df['gdp_usd'] * get_conversion_factor()
#df = df.set_index('year')

# insert the data into the database
for year, row in df.iterrows():
    query = "INSERT OR REPLACE INTO india_gdp (year, gdp_usd, gdp_inr) VALUES (?, ?, ?)"
    values = (row['year'], row['gdp_usd'], row['gdp_inr'])
    cursor.execute(query, values)

conn.commit()
conn.close()
print("Data inserted successfully into the database.")

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.widgets import Button

# Connect to database and read the data
conn = sqlite3.connect(r'model\\india_gdp.db')
df = pd.read_sql('SELECT * FROM india_gdp', conn, index_col='year')

# Create the chart
fig, ax = plt.subplots()
line, = ax.plot(df.index, df['gdp_usd'], marker='o')
ax.set_xlabel('Year')
ax.set_title('India GDP (1987-2027)')

# Set x-axis ticks to every 2 years
ax.set_xticks(df.index[::2])

# Prepend a * to the label for years greater than or equal to 2023
xtick_labels = [f'{year}*' if year >= 2023 else str(year) for year in df.index[::2]]
ax.set_xticklabels(xtick_labels)

# Rotate x-axis tick labels
ax.tick_params(axis='x', rotation=45)

# Set initial y-axis limits
ax.set_ylim([0, df['gdp_usd'].max() * 1.1])

# Add USD value annotations
for x, y in zip(df.index, df['gdp_usd']):
    ax.text(x, y * 1.02, f'{y:.2f} (USD)', rotation=70, ha='left', va='bottom', fontsize=7)

# Add a button to switch between USD and INR
def switch_currency(event):
    # Get the current label of the button
    label = event.inaxes.get_title()

    if label == 'Switch to INR':
        # Switch to INR
        line.set_ydata(df['gdp_inr'])
        ax.set_ylabel('GDP (INR 100 Crores)')
        event.inaxes.set_title('Switch to USD')
        # Remove existing USD value annotations
        for txt in ax.texts:
            if txt.get_text().endswith('USD)'):
                txt.set_visible(False)
        # Add INR value annotations
        for x, y in zip(df.index, df['gdp_inr']):
            ax.text(x, y * 1.02, f'{y:.2f} (INR)', rotation=70, ha='left', va='bottom', fontsize=7)
        # Update y-axis limits
        ax.set_ylim([0, df['gdp_inr'].max() * 1.1])
    else:
        # Switch to USD
        line.set_ydata(df['gdp_usd'])
        ax.set_ylabel('GDP (USD billions)')
        event.inaxes.set_title('Switch to INR')
        # Remove existing INR value annotations
        for txt in ax.texts:
            if txt.get_text().endswith('INR)'):
                txt.set_visible(False)
        # Add USD value annotations
        for x, y in zip(df.index, df['gdp_usd']):
            ax.text(x, y * 1.02, f'{y:.2f} (USD)', rotation=70, ha='left', va='bottom', fontsize=7)
        # Update y-axis limits
        ax.set_ylim([0, df['gdp_usd'].max() * 1.1])

    # Update the chart
    fig.canvas.draw_idle()

# Add the button to the chart
button_ax = fig.add_axes([0.85, 0.9, 0.1, 0.075])  # move the button to top right
button_label = 'Conversion Factor'
button = Button(button_ax, button_label)
button.on_clicked(switch_currency)

# Show the chart
plt.show()
