import pytest
import pandas as pd

from pathlib import Path
from datetime import datetime
from libApi.pricers.pricer import Pricer


def test_log_api_call(tmp_path, monkeypatch):
    """
    Test log_api_call writes entries correctly and appends multiple calls.
    """

    # Setup: temporary CSV file path for the log
    log_file = tmp_path / "test_log.csv"

    # Patch the constant in pricer to point to the temporary log file
    from libApi import pricers
    monkeypatch.setattr(pricers.pricer, "PRICING_LOG_FILE_PATH", str(log_file))

    pricer = Pricer()

    # --- 1. Call log_api_call once and check content ---
    pricer.log_api_call(3)

    df = pd.read_csv(log_file)
    assert len(df) == 1, "Log should contain exactly one entry after first call"
    assert df.iloc[0]["n_instruments"] == 3, "Logged n_instruments must match input"
    assert "date" in df.columns, "Log file must contain a 'date' column"
    assert pd.to_datetime(df.iloc[0]["date"], errors='coerce') is not pd.NaT, "Date must be a valid timestamp"

    # --- 2. Call log_api_call again to test appending behavior ---
    pricer.log_api_call(7)

    df2 = pd.read_csv(log_file)
    assert len(df2) == 2, "Log should append new entry, total 2 entries now"
    assert df2.iloc[1]["n_instruments"] == 7, "Second log entry must match second input"
    assert pd.to_datetime(df2.iloc[1]["date"], errors='coerce') is not pd.NaT, "Date in second entry must be valid"

    # --- 3. Test that log entries are in chronological order (optional) ---
    dates = pd.to_datetime(df2["date"])
    assert dates.is_monotonic_increasing, "Log dates should be sorted in ascending order"

    # --- 4. Edge case: log_api_call with zero instruments ---
    pricer.log_api_call(0)
    df3 = pd.read_csv(log_file)
    assert len(df3) == 3, "Log should have 3 entries after third call"
    assert df3.iloc[2]["n_instruments"] == 0, "Third log entry should record zero instruments"

    # --- 5. Edge case: large number of instruments ---
    large_number = 10_000
    pricer.log_api_call(large_number)
    df4 = pd.read_csv(log_file)
    assert len(df4) == 4, "Log should have 4 entries after fourth call"
    assert df4.iloc[3]["n_instruments"] == large_number, "Fourth entry should record large number"



def test_split_list () :
    """
    
    """
    pricer = Pricer()

    # Normal case: list of 10 elements split into chunks of max size 4
    result_1 = pricer.split_list(list(range(10)), 4)
    assert len(result_1) == 3
    assert result_1[0] == [0, 1, 2, 3]
    assert result_1[1] == [4, 5, 6, 7]
    assert result_1[2] == [8, 9]

    # Edge case: empty list should return empty list of chunks
    result_2 = pricer.split_list([], 10)
    assert result_2 == []

    # Error case: passing None instead of list should raise TypeError
    with pytest.raises(TypeError):
        pricer.split_list(None, 10)

    # Error case: max_num = 0 should raise ZeroDivisionError (division by zero)
    with pytest.raises(ZeroDivisionError):
        pricer.split_list([1, 2, 3], 0)

    # Case where max_num is greater than the length of the list: should return one chunk containing the whole list
    result_3 = pricer.split_list([1, 2, 3], 10)
    assert result_3 == [[1, 2, 3]]

    # Case max_num = 1: each element should be in its own chunk
    lst = list(range(5))
    result_4 = pricer.split_list(lst, 1)
    assert result_4 == [[0], [1], [2], [3], [4]]

    # Case max_num equals list length: only one chunk with the entire list
    result_5 = pricer.split_list(lst, len(lst))
    assert result_5 == [lst]

    # Case empty list with max_num > 0: should still return empty list of chunks
    result_6 = pricer.split_list([], 5)
    assert result_6 == []

    # Single element list: should return a list with one chunk containing that element
    result_7 = pricer.split_list([42], 3)
    assert result_7 == [[42]]



def test_treat_json_response_pricer () :
    """
    
    """
    pricer = Pricer()

    json_response = {
        "instruments": [
            {
                "id": 1,
                "results": [
                    {"code": "PRICE", "value": 100, "currency": "USD"},
                    {"code": "YIELD", "value": 0.05}
                ],
                "assets": [
                    {
                        "name": "Asset1",
                        "results": [
                            {"code": "VOL", "value": 0.2}
                        ]
                    }
                ]
            }
        ]
    }

    instruments = [
        {"ID": 1, "direction": "BUY", "pair": "EUR/USD", "opt_type": "call", "strike": 1.2, 
         "notional": 1000000, "notional_currency": "EUR", "expiry": "2025-12-31", 
         "BBGTicker": "EURUSD Curncy", "stratid": "abc123"}
    ]

    result = pricer.treat_json_response_pricer(json_response, instruments)

    assert "PRICE" in result.columns
    assert result.loc[0, "PRICE"] == 100
    assert result.loc[0, "PRICE_currency"] == "USD"
    assert "VOL" in result.columns
    assert result.loc[0, "VOL"] == 0.2
    assert result.loc[0, "direction"] == "BUY"