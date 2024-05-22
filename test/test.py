import os
import requests
import json
import pandas as pd
import time 

# Path to your CSV file
csv_file = 'C:\\Users\\ahu\\source\\repos\\S2_Research\\S2_Research\\item.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file)

# Extract all series IDs from the DataFrame and prefix them with "CUUR0000"
series_ids = ['CUUR0000' + code for code in df['item_code'].tolist()]
item_names = df['item_name'].tolist()
# BLS API URL
api_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

# Headers for the API request
headers = {'Content-type': 'application/json'}

# Function to fetch data for a list of series IDs
def fetch_data(series_ids_chunk):
    data = json.dumps({
        "seriesid": series_ids_chunk,
        "startyear": "2024",
        "endyear": "2024",
        "catalog": False,
        "calculations": False,
        "annualaverage": False,
        "aspects": False,
        "registrationkey": "5b52a43aae7745ad8b0b4d35bea1c65e"
    })
    
    response = requests.post(api_url, data=data, headers=headers)
    return response.json()

# Split the series IDs into chunks of 24
chunk_size = 24
series_id_chunks = [series_ids[i:i + chunk_size] for i in range(0, len(series_ids), chunk_size)]

# Initialize an empty list to store records
all_records = []

# Fetch data for each chunk and process the JSON response
for chunk in series_id_chunks:
    time.sleep(1)
    json_data = fetch_data(chunk)
    
    # Check for errors in the response
    if json_data.get('status') != 'REQUEST_SUCCEEDED':
        print(f"Request failed: {json_data.get('message')}")
    else:
        # Process the JSON response to extract data for April
        for series in json_data['Results']['series']:

            series_id = series['seriesID']
            for item in series['data']:
                year = item['year']
                period = item['period']
                value = item['value']
                footnotes = ""
                for footnote in item['footnotes']:
                    if footnote:
                        footnotes += footnote['text'] + ','
                if period == 'M04':  # Filter for April
                    date_str = f"{year}-04"
                    all_records.append([series_id, date_str, float(value)])

# Convert the records into a DataFrame
df_records = pd.DataFrame(all_records, columns=["Series_ID", "Date", "CPI Value"])

# Convert the Date column to datetime format (YYYY-MM) and then format it as a string
df_records['Date'] = pd.to_datetime(df_records['Date'], format='%Y-%m').dt.strftime('%Y-%m')

# Save the DataFrame to a CSV file
output_csv_file = 'C:\\Users\\ahu\\source\\repos\\S2_Research\\S2_Research\\cpi_data_april_2024.csv'
df_records.to_csv(output_csv_file, index=False)
print(f"Data saved to {output_csv_file}")
