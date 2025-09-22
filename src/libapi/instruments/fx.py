from __future__ import annotations

import datetime as dt

from itertools import product
from typing import Dict, List, Optional, Sequence

from libapi.config.parameters import CCYS_ORDER
from libapi.utils.formatter import date_to_str
from libapi.utils.validators import validate_direction, validate_flat_strikes


def find_ccy (ccy : str, ccys_order : Optional[List] = None) :
    """
    
    """
    ccys_order = CCYS_ORDER if ccys_order is None else ccys_order
    
    for i in range(len(CCYS_ORDER)):
    
        if ccys_order[i] in ccy :
            return ccys_order[i]
    
    return None


def make_fx_option_leg_payload (
        
        direction : str,
        ccy_pair : str,
        opt_type : str,
        strike : float | str,
        expiry : str | dt.datetime,
        strait_id : Optional[str | int] = None,
        notional : float = 1_000_000
    
    ) -> Dict :
    """
    
    """
    instrument = {

        'direction' : direction,
        'pair' : ccy_pair,
        'opt_type' : opt_type,
        'strike' : strike,
        'notional' : notional,
        'notional_currency' : find_ccy(ccy_pair),
        'expiry' : date_to_str(expiry),
        "stratid" : str(strait_id) if strait_id else None

    }

    return instrument


def make_fx_straddle_payloads (
        
        ccys : List[str],
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
    instruments = make_fx_option_legs_from_combos(ccys, expiries, strikes, options, direction)

    return instruments


def make_fx_strangle_payloads (
        
        ccys : List[str],
        expiries : List[str | dt.datetime],
        strikes : List,
        direction : str = "Sell"
    
    ) -> List[Dict] :
    """
    """
    validate_flat_strikes(strikes)
    
    # Validate direction
    validate_direction(direction)

    instruments = []
    P, C = "Put", "Call"
    expiries_s = [date_to_str(d) for d in expiries]

    # Create all instruments to price
    for strat_id, (ccy, strike, expiry) in enumerate(product(ccys, strikes, expiries_s)) :

        s1, s2 = strike[0], strike[1]

        instruments.append(make_fx_option_leg_payload(direction, ccy, P, s1, expiry, strat_id))
        instruments.append(make_fx_option_leg_payload(direction, ccy, C, s2, expiry, strat_id))

    return instruments


def make_fx_put_leg_payloads (
    
        ccys : List[str],
        expiries : List[str | dt.datetime | dt.date],
        strikes : List[str | int],
        options : Sequence[str] = ("Put",),
        direction : str = "Sell",
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_fx_option_legs_from_combos(ccys, expiries, strikes, options, direction)

    return instruments


def make_fx_call_leg_payloads (
        
        ccys : List[str],
        expiries : List[str | dt.datetime],
        strikes : List,
        options : Sequence[str] = ("Call",),
        direction : str = "Sell",
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_fx_option_legs_from_combos(ccys, expiries, strikes, options, direction)

    return instruments


def make_fx_call_spread_payloads (
        
        ccys : List[str],
        expiries : List[str | dt.datetime],
        strikes : List[str | int],
        #options : Sequence[str] = ("Call",),
        direction : str = "Sell",
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_fx_option_legs_from_strike_pairs(ccys, expiries, strikes, direction=direction)

    return instruments


def make_fx_put_spread_payloads (
        
        ccys : List[str],
        expiries : List[str | dt.datetime],
        strikes : List[str | int],
        #options : Sequence[str] = ("Call",),
        direction : str = "Buy",
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_fx_option_legs_from_strike_pairs(ccys, expiries, strikes, opt_type="Put", direction=direction)

    return instruments



def make_fx_option_legs_from_combos (
        
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
            instruments.append(make_fx_option_leg_payload(direction, underlier, opt, strike, expiry, expiry, strat_id))
    
    return instruments


def make_fx_option_legs_from_strike_pairs (
    
        ccys : List[str],
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
    for strat_id, (date, ccy, strike) in enumerate(product(expiries_s, ccys, strikes)) :

        s1 = strike[0]
        s2 = strike[1]

        instruments.extend(
            [
                make_fx_option_leg_payload(direction, ccy, opt_type, s1, date, date, strat_id),
                make_fx_option_leg_payload('Buy' if direction == "Sell" else "Sell", ccy, opt_type, s2, date, date, strat_id)
            ]

        )

    return instruments
