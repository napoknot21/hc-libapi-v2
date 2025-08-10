import pytest
import pandas as pd

from unittest.mock import MagicMock, patch
from libapi.pricers.fx import PricerFX


@pytest.fixture
def pricer_fx () :
    """
    """
    fx = PricerFX()
    fx.api.post = MagicMock()  # Mock API calls
    
    return fx


def test_create_json_for_instruments (pricer_fx) :
    """
    
    """
    payload = pricer_fx.create_json_for_instruments(

        direction="Buy",
        pair="EURUSD",
        opt_type="Call",
        strike="100%",
        notional=1_000_000,
        notional_currency="EUR",
        expiry="2024-12-31",
        ID="abc123"
    
    )

    assert payload["InstrumentType"] == "Vanilla"
    assert payload["UnderlyingAsset"]["BaseCurrency"] == "EUR"
    assert payload["UnderlyingAsset"]["TermCurrency"] == "USD"
    assert payload["BuySell"] == "Buy"
    assert payload["CallPut"] == "Call"
    assert payload["Strike"] == "100%"
    assert payload["ID"] == "abc123"


def test_post_request_price (pricer_fx) :
    """
    
    """
    # Mock API response
    mock_response = {
        "instruments": [
            {
                "id": 0,
                "results": [
                    {
                        "code": "MarketPriceAsk",
                        "value": 1.23,
                        "currency": "EUR"
                    }
                ]
            }
        ]
    }

    pricer_fx.api.post.return_value = mock_response

    instruments = [
        {
            "direction": "Sell",
            "pair": "EURUSD",
            "opt_type": "Call",
            "strike": "100%",
            "notional": 1_000_000,
            "notional_currency": "EUR",
            "expiry": "2024-12-31",
            "ID": None,
        }
    ]

    result_df = pricer_fx.post_request_price(instruments, time="12:00", date="2024-12-01")

    assert isinstance(result_df, pd.DataFrame)
    assert "MarketPriceAsk" in result_df.columns
    assert result_df.iloc[0]["MarketPriceAsk"] == 1.23