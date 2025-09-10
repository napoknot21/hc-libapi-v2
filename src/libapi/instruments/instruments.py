import datetime as dt
from typing import Dict, Optional, List, Sequence
from itertools import product

from libapi.utils.formatter import date_to_str
from libapi.config.parameters import CCYS_ORDER


def find_ccy (ccy : str, ccys_order : Optional[List] = None) :
    """
    
    """
    ccys_oder = CCYS_ORDER if ccys_order is None else ccys_order
    
    for i in range(len(CCYS_ORDER)):
    
        if ccys_order[i] in ccy :
            return ccys_order[i]
    
    return None


def make_vanilla_option_payloads (
        
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
    _validate_flat_strikes(strikes)

    # Validate direction
    _validate_direction(direction)

    expiries_s = [date_to_str(d) for d in expiries]
    instruments: List[Dict] = []

    for strat_id, (expiry, underlier, strike) in enumerate(product(expiries_s, underliers, strikes)) :

        for opt in opt_types :
            instruments.append(make_bbg_option_payload(direction, underlier, opt, strike, expiry, expiry, strat_id))
    
    return instruments


# -------------------------------------------------- FX instruments -------------------------------------------------- #


def create_instrument_dict_ccys (direction : str, ccy : str, opt_type, strike : float, expiry, strait_id, notional : float = 1_000_000) :
    """
    
    """
    instrument = {

        'direction' : direction,
        'pair' : ccy,
        'opt_type' : opt_type,
        'strike' : strike,
        'notional' : notional,
        'notional_currency' : find_ccy(ccy),
        'expiry' : expiry,
        "stratid" : strait_id

    }

    return instrument


# TODO RENAMEEEEEEEE

def get_instruments_samestrike_sell_put_call_fx (ccys, expiries, strikes, direction="Sell", options : Sequence[str] = ("Put", "Call")) :
    """
    
    """
    instruments = make_vanilla_option_payloads(ccys, expiries, strikes, options, direction)

    return instruments


def get_instruments_2_strikes_sell_put_call_fx (ccys, expiries, strikes, direction="Sell") :
    """
    Create instruments for a strangle strategy
    here strikes is a list of tuples, each tuple contains two strikes,
    one for the put and one for the call
    """
    if type(strikes[0]) != tuple and type(strikes[0]) != list :
        raise KeyError("[-] Strikes should be a list of tuples or lists, not a list of single values")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for ccy in ccys :

            for strike in strikes :

                s1 = strike[0]
                s2 = strike[1]
                
                instruments.extend(
                    [
                        create_instrument_dict_ccys(direction, ccy, 'Put', s1, date, stratid),
                        create_instrument_dict_ccys(direction, ccy, 'Call', s2, date, stratid)
                    ]
                )

                stratid += 1

    return instruments


def get_instruments_put_fx(ccys, expiries, strikes, direction="Sell", options : Sequence[str] = ("Put",)):
    """

    """
    instruments = make_vanilla_option_payloads(ccys, expiries, strikes, options, direction)

    return instruments


def get_instruments_call_fx (ccys, expiries, strikes, direction="Sell", options : Sequence[str] = ("Call",)) :
    """
    
    """
    instruments = make_vanilla_option_payloads(ccys, expiries, strikes, options, direction)

    return instruments


def get_instruments_sell_cs_fx (ccys, expiries, strikes, direction="Sell") :
    """

    """
    if type(strikes[0]) != tuple and type(strikes[0]) != list :
        # 
        raise KeyError("[-] Strikes should be a list of tuples or lists, not a list of single values")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for ccy in ccys :

            for strike in strikes :

                s1 = strike[0]
                s2 = strike[1]
                
                instruments.extend(
                    [
                        create_instrument_dict_ccys(direction, ccy, 'Call', s1, date, stratid),
                        create_instrument_dict_ccys('Buy' if direction == "Sell" else "Sell", ccy, 'Call', s2, date, stratid)
                    ]
                )
                stratid += 1

    return instruments


def get_instruments_sell_ps_fx (ccys, expiries, strikes, direction="Sell") :
    """
    
    """
    if type(strikes[0])!=tuple and type(strikes[0])!=list:
        raise KeyError("Strikes should be a list of tuples or lists, not a list of single values")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for ccy in ccys :

            for strike in strikes :

                s1 = strike[0]
                s2 = strike[1]
                
                instruments.extend(
                    [
                        create_instrument_dict_ccys('Buy' if direction == "Sell" else "Sell", ccy, 'Put', s1, date, stratid),
                        create_instrument_dict_ccys(direction, ccy, 'Put', s2, date, stratid)
                    ]
                )
                stratid += 1

    return instruments


# -------------------------------------------------- Equity instruments -------------------------------------------------- #

def make_bbg_option_payload (
    
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
    Build a normalized JSON payload describing an option instrument identified by a Bloomberg ticker.

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
        'notional' : notional,
        'notional_currency' : notional_ccy,
        'expiry' : expiry,
        "stratid" : str(strat_id),
        "SettlementDate" : settlement_date
    }

    return instrument


def make_bbg_put_call_pairs_eq (
    
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
    instruments = make_vanilla_option_payloads(bbg_tickers, expiries, strikes, options, direction)

    return instruments


def make_bbg_strangle_payloads_eq (
    
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
    _validate_flat_strikes(strikes)
    
    # Validate direction
    _validate_direction(direction)

    instruments = []
    P, C = "Put", "Call"
    expiries_s = [date_to_str(d) for d in expiries]

    # Create all instruments to price
    for strat_id, (bbg_ticker, strike, expiry) in enumerate(product(bbg_tickers, strikes, expiries_s)) :

        s1, s2 = strike[0], strike[1]

        instruments.append(make_bbg_option_payload(direction, bbg_ticker, P, s1, expiry, expiry, strat_id))
        instruments.append(make_bbg_option_payload(direction, bbg_ticker, C, s2, expiry, expiry, strat_id))

    return instruments


def make_bbg_put_payloads_eq (
    
        bbg_tickers : List[str],
        expiries : List[str | dt.datetime],
        strikes : List,
        direction : str = "Sell",
        options : Sequence[str] = ("Put",)
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_vanilla_option_payloads(bbg_tickers, expiries, strikes, options, direction)

    return instruments


def make_bbg_call_payloads_eq (
        
        bbg_tickers : List[str],
        expiries : List[str | dt.datetime],
        strikes : List,
        direction : str = "Sell",
        options : Sequence[str] = ("Call",)
    
    ) -> List[Dict] :
    """
    
    """
    instruments = make_vanilla_option_payloads(bbg_tickers, expiries, strikes, options, direction)

    return instruments
    

def get_instruments_sell_cs_eq (BBG_tickers, expiries, strikes, direction="Sell") :
    """
    
    """
    if type(strikes[0]) != tuple and type(strikes[0]) != list :
        raise KeyError("[-] Strikes should be a list of tuples or lists, not a list of single values")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for bbg_ticker in BBG_tickers :

            for strike in strikes :

                s1 = strike[0]
                s2 = strike[1]

                instruments.extend(
                    [
                        make_bbg_option_payload(direction, bbg_ticker, 'Call', s1, date, date, stratid),
                        make_bbg_option_payload('Buy' if direction=="Sell" else "Sell", bbg_ticker, 'Call', s2, date, date, stratid)
                    ]
                )

                stratid += 1

    return instruments


def get_instruments_sell_ps_eq (BBG_tickers, expiries, strikes : list, direction : str = "Sell") :
    """
    
    """
    if type(strikes[0]) != tuple and type(strikes[0]) != list :
        raise KeyError("Strikes should be a list of tuples or lists, not a list of single values")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for bbg_ticker in BBG_tickers :

            for strike in strikes :

                s1 = strike[0]
                s2 = strike[1]

                instruments.extend(
                    [
                        make_bbg_option_payload('Buy' if direction=="Sell" else "Sell", bbg_ticker, 'Put', s1, date, date, stratid),
                        make_bbg_option_payload(direction, bbg_ticker, 'Put', s2, date, date, stratid)
                    ]
                )
                
                stratid += 1

    return instruments




def _validate_flat_strikes (strikes : List) :
    """
    
    """
    if not strikes:
        return
    
    if not isinstance(strikes[0], (tuple, list)) :
        raise ValueError("`strikes` must be a flat list of single values, not tuples/lists.")
    

def _validate_direction (direction: str) -> None :
    """
    
    """
    if direction not in {"Buy", "Sell"} :
        raise ValueError('`direction` must be "Buy" or "Sell".')
    

def _opp (direction: str) -> str :
    """
    
    """
    return "Sell" if direction == "Buy" else "Buy"