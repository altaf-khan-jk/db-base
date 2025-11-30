import requests
import pandas as pd

def fetch_climate_data():
    url = "https://api.worldbank.org/v2/country/CAN/indicator/EN.CLC.MDAT.ZS?format=json"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API fetch failed with status code {response.status_code}")

    data = response.json()

    try:
        records = data[1]  # API returns a wrapper: [metadata, data]
        df = pd.DataFrame(records)
        print("Sample Climate Data (First 5):")
        print(df.head())
        return df
    except Exception as e:
        print("Error processing API response:", e)
        raise

def main():
    print("Starting ETL - Phase 1: Fetch Climate Data")
    df = fetch_climate_data()
    print("Data Fetch Successful!")

if __name__ == "__main__":
    main()
