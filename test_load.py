# test_load.py
import pendulum
import pandas as pd
from src.load import load_data

df = pd.DataFrame([{
    "T": "AAPL",
    "V": 12345,
    "VW": 180.5,
    "O": 179.2,
    "C": 181.0,
    "H": 182.0,
    "L": 178.5,
    "N": 2100,
    "DATE": "2025-10-24"
}])

run_id = f"manual_test_{pendulum.now().int_timestamp}"
load_data(df, "2025-10-24", run_id)
