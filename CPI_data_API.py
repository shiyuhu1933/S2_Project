import os
import requests
import json
import pandas as pd
import time

class CPIDataFetcher:
    def __init__(self, csv_file, output_csv_file, start_year, end_year):
        """
        Initialize the CPIDataFetcher with paths, years, and API configuration.

        :param csv_file: Path to the input CSV file containing item codes and names.
        :param output_csv_file: Path to the output CSV file where results will be saved.
        :param start_year: Start year for fetching CPI data.
        :param end_year: End year for fetching CPI data.
        """
        self.csv_file = csv_file
        self.output_csv_file = output_csv_file
        self.start_year = start_year
        self.end_year = end_year
        self.api_url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
        self.headers = {'Content-type': 'application/json'}
        self.df = pd.read_csv(csv_file)
        self.series_ids = ['CUUR0000' + code for code in self.df['item_code'].tolist()]

    def fetch_data(self, series_ids_chunk):
        """
        Fetch data for a list of series IDs from the BLS API.

        :param series_ids_chunk: List of series IDs to fetch data for.
        :return: JSON response from the API.
        """
        data = json.dumps({
            "seriesid": series_ids_chunk,
            "startyear": self.start_year,
            "endyear": self.end_year,
            "catalog": False,
            "calculations": False,
            "annualaverage": False,
            "aspects": False,
            "registrationkey": "5b52a43aae7745ad8b0b4d35bea1c65e"
        })
        
        response = requests.post(self.api_url, data=data, headers=self.headers)
        return response.json()

    def process_data(self):
        """
        Process the data by fetching it from the BLS API and organizing it into a DataFrame.

        :return: DataFrame containing the processed CPI data.
        """
        chunk_size = 24
        series_id_chunks = [self.series_ids[i:i + chunk_size] for i in range(0, len(self.series_ids), chunk_size)]
        all_records = []

        for chunk in series_id_chunks:
            time.sleep(1)
            json_data = self.fetch_data(chunk)
            
            if json_data.get('status') != 'REQUEST_SUCCEEDED':
                print(f"Request failed: {json_data.get('message')}")
            else:
                for series in json_data['Results']['series']:
                    series_id = series['seriesID']
                    item_name = self.df.loc[self.df['item_code'] == series_id[8:], 'item_name'].values[0]
                    for item in series['data']:
                        year = item['year']
                        period = item['period']
                        value = item['value']
                        footnotes = ""
                        for footnote in item['footnotes']:
                            if footnote:
                                footnotes += footnote['text'] + ','
                        month = period[1:]
                        date_str = f"{year}-{month}"
                        all_records.append([series_id, item_name, date_str, float(value)])

        df_records = pd.DataFrame(all_records, columns=["Series_ID", "Item_name", "Date", "CPI_value"])
        df_records['Date'] = pd.to_datetime(df_records['Date'], format='%Y-%m')
        return df_records

    def save_to_csv(self, df):
        """
        Save the DataFrame to a CSV file.

        :param df: DataFrame to be saved.
        """
        df.to_csv(self.output_csv_file, index=False)
        print(f"Data saved to {self.output_csv_file}")

if __name__ == '__main__':
    csv_file = 'C:\\Users\\ahu\\source\\repos\\S2_Research\\S2_Research\\item.csv'
    output_csv_file = 'C:\\Users\\ahu\\source\\repos\\S2_Research\\S2_Research\\cpi_data_2019_2024.csv'
    start_year = 2020
    end_year = 2024

    fetcher = CPIDataFetcher(csv_file, output_csv_file, start_year, end_year)
    df_records = fetcher.process_data()
    fetcher.save_to_csv(df_records)





    
