import os
import traceback
import pandas as pd
from dotenv import load_dotenv
from entsoe import EntsoePandasClient

load_dotenv()
api_key = os.getenv("ENTSO_E_API_KEY")
print(f"Using API key: {api_key[:5]}...{api_key[-5:] if api_key else 'None'}")

client = EntsoePandasClient(api_key=api_key)
start = pd.Timestamp("2023-01-01", tz="Europe/Berlin")
end = pd.Timestamp("2023-01-02", tz="Europe/Berlin")
COUNTRY_CODE = "DE_LU"

try:
    print(f"Testing query_day_ahead_prices for {COUNTRY_CODE}...")
    s = client.query_day_ahead_prices(COUNTRY_CODE, start=start, end=end)
    print("SUCCESS")
    print(s.head())
except Exception:
    print("FAILED")
    traceback.print_exc()
