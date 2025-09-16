from __future__ import annotations

import datetime as dt

from itertools import product
from typing import Optional, List, Dict, Sequence

from libapi.utils.validators import validate_flat_strikes, validate_direction
from libapi.utils.formatter import date_to_str


def make_eq_option_leg_payload (
    
        direction : str,
        bbg_ticker : str,
        opt_type : str,
        strike : str | int,
        expiry : str | dt.datetime,
        settlement_date : str | dt.datetime,
        strat_id : str | int,
        notional : float = 1_000_000,
        notional_ccy : str = "EUR"
    
    ) -> Optional[Dict] :
    """
    Build a normalized JSON payload describing an Equity option instrument identified by a Bloomberg ticker.

    Args:
        direction (str) : Trade direction, e.g. "Buy" or "Sell".
        bbg_ticker (str) : Bloomberg ticker of the underlier/instrument (e.g., "SPX Index")
        opt_type (str) :  Option type, e.g. "Put" or "Call".
        strike (str | int) : Option strike; coerced to string in the output.
        expiry (str | dt.datetime) : Option expiry (maturity). Accepted as string or datetime; converted to string.
        strat_id (str | int) : Strategy identifier used to tag the trade.
        notional (int) : Trade notional (face amount).
        notional_ccy (str) : Notional currency (ISO code), e.g. "EUR", "USD".

    Returns :
        - instrument : Dict -> Json instrument
    """
    # We assume that expiry and settelment date are already in string format
    instrument = {

        'direction' : direction,
        'BBGTicker' : bbg_ticker,
        'opt_type' : opt_type,
        'strike' : str(strike),
        'notional' : str(notional),
        'notional_currency' : notional_ccy,
        'expiry' : expiry,
        "stratid" : str(strat_id),
        "SettlementDate" : settlement_date
    
    }

    return instrument


def make_eq_straddle_payloads (
    
        bbg_tickers : List[str],
        expiries : List[str | dt.datetime],
        strikes : List[str | int],
        direction : str = "Sell",
        options : Sequence[str] = ("Put", "Call")
    
    ) -> List[Dict] :
    """
    This function makes a Stranddle payloads

    Args:
        bbg_tickers (List[str]): List/iterable of Bloomberg tickers (e.g., "SPX Index").
        expiries (List[str | datetime]): List/iterable of option expiries (as str or datetime).
        strikes (List[str | int | float]): List/iterable of strikes (must be atomic values).
        direction (str, optional): Trade direction for both legs ("Sell" by default or "Buy").

    Returns:
        instruments (list[dict] | None): A list of JSON payloads representing option instruments. 
            For each (ticker, expiry, strike) combination, two entries are created, one Put and one Call.
    """
    instruments = make_eq_option_legs_from_combos(bbg_tickers, expiries, strikes, options, direction)

    return instruments


def make_eq_strangle_payloads (
    
        bbg_tickers : List[str],
        expiries : List[str | dt.datetime],
        strikes : List,
        direction : str = "Sell"
    
    ) -> List[Dict] :
    """
    Create instruments for a strangle strategy
    here strikes is a list of tuples, each tuple contains two strikes, 
    one for the put and one for the call
    """
    validate_flat_strikes(strikes)
    
    # Validate direction
    validate_direction(direction)

    instruments = []
    P, C = "Put", "Call"
    expiries_s = [date_to_str(d) for d in expiries]

    # Create all instruments to price
    for strat_id, (bbg_ticker, strike, expiry) in enumerate(product(bbg_tickers, strikes, expiries_s)) :

        s1, s2 = strike[0], strike[1]

        instruments.append(make_eq_option_leg_payload(direction, bbg_ticker, P, s1, expiry, expiry, strat_id))
        instruments.append(make_eq_option_leg_payload(direction, bbg_ticker, C, s2, expiry, expiry, strat_id))

    return instruments


def make_eq_put_leg_payloads (
    
        bbg_tickers : List[str],
        expiries : List[str | dt.datetime],
        strikes : List,
        direction : str = "Sell",
        options : Sequence[str] = ("Put",)
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_eq_option_legs_from_combos(bbg_tickers, expiries, strikes, options, direction)

    return instruments


def make_eq_call_leg_payloads (
        
        bbg_tickers : List[str],
        expiries : List[str | dt.datetime],
        strikes : List,
        direction : str = "Sell",
        options : Sequence[str] = ("Call",)
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_eq_option_legs_from_combos(bbg_tickers, expiries, strikes, options, direction)

    return instruments
    

def make_eq_call_spread_payloads (bbg_tickers : List[str], expiries : List[str | dt.datime | dt.date], strikes : List[str | int], direction : str = "Sell") :
    """
    Build a list of call spread instruments for equity underliers.

    Each strike pair defines:
        - Buy Call at lower strike
        - Sell Call at higher strike

    If direction == "Sell", logic is reversed:
        - Sell Call at lower strike
        - Buy Call at higher strike

    Args:
        bbg_tickers (List[str]): Bloomberg tickers.
        expiries (List[str | datetime]): Expiry dates.
        strike_pairs (List[Tuple[float, float]]): Each pair defines (lower_strike, higher_strike).
        direction (str): "Buy" or "Sell" the spread (default = "Sell").

    Returns:
        List[Dict]: Payloads for both legs of each spread.
    """
    instruments = make_eq_option_legs_from_strike_pairs(bbg_tickers, expiries, strikes, direction=direction)

    return instruments


def make_eq_put_spread_payloads (bbg_tickers, expiries, strikes : List, direction : str = "Buy") :
    """
    
    """
    instruments = make_eq_option_legs_from_strike_pairs(bbg_tickers, expiries, strikes, opt_type="Put", direction=direction)

    return instruments


# --------------------------------- Helpers for combos and strike Pairs ---------------------------------


def make_eq_option_legs_from_combos (
        
        underliers: List[str],
        expiries: List[str | dt.datetime],
        strikes: List,
        opt_types: Sequence[str] = ("Put",),   # can pass ("Call",) or ("Put","Call")
        direction: str = "Sell",
    
    ) -> List[Dict]:
    """
    Generic builder for equity vanilla option payloads (Bloomberg underliers).

    Args:
        bbg_tickers: Underlier tickers (e.g., "SPX Index", "AAPL US Equity").
        expiries: Expiry dates (str/date/datetime); settlement defaults to expiry.
        strikes: Flat list of strikes (no tuples/lists).
        opt_types: Which option types to create per combo, e.g. ("Put",), ("Call",), ("Put","Call").
        direction: "Sell" (default) or "Buy".

    Returns:
        list[dict]: One payload per (expiry, ticker, strike, opt_type).
    """
    # Validate strikes: flat list (no lists/tuples inside)
    validate_flat_strikes(strikes)

    # Validate direction
    validate_direction(direction)

    expiries_s = [date_to_str(d) for d in expiries]
    instruments : List[Dict] = []

    for strat_id, (expiry, underlier, strike) in enumerate(product(expiries_s, underliers, strikes)) :

        for opt in opt_types :
            instruments.append(make_eq_option_leg_payload(direction, underlier, opt, strike, expiry, expiry, strat_id))
    
    return instruments


def make_eq_option_legs_from_strike_pairs (
    
        bbg_tickers : List[str],
        expiries : List[str | dt.datime | dt.date],
        strikes : List[str | int],
        opt_type : str = "Call",
        direction : str = "Sell"
    
    ) -> List[Dict] :
    """
    
    """
    if type(strikes[0]) != tuple and type(strikes[0]) != list :
        raise KeyError("[-] Strikes should be a list of tuples or lists, not a list of single values")
    
    expiries_s = [date_to_str(date) for date in expiries]
    instruments = []

    # Create all instruments to price
    for strat_id, (date, bbg_ticker, strike) in enumerate(product(expiries_s, bbg_tickers, strikes)) :

        s1 = strike[0]
        s2 = strike[1]

        instruments.extend(
            [
                make_eq_option_leg_payload(direction, bbg_ticker, opt_type, s1, date, date, strat_id),
                make_eq_option_leg_payload('Buy' if direction == "Sell" else "Sell", bbg_ticker, opt_type, s2, date, date, strat_id)
            ]

        )

    return instruments
