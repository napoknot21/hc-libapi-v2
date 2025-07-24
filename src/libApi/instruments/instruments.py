CCYS_ORDER=['EUR', 'USD', 'CHF', 'CAD', 'JPY', 'GBP', 'SEK', 'NOK']


def find_ccy (ccy : str) :
    """
    
    """
    for i in range(len(CCYS_ORDER)):
    
        if CCYS_ORDER[i] in ccy:
            return CCYS_ORDER[i]
    
    return None


def create_instrument_dict_ccys (direction, ccy, opt_type, strike, expiry, strait_id, notional=1_000_000) :
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


# ------------------- FX instruments -------------------#

def get_instruments_samestrike_sell_put_call_fx (ccys, expiries, strikes, direction="Sell") :
    """
    
    """
    if type(strikes[0]) == tuple or type(strikes[0]) == list :
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for ccy in ccys :
            
            for strike in strikes :
                
                instruments.extend(
                    [
                        create_instrument_dict_ccys(direction, ccy, 'Put', strike, date, stratid),
                        create_instrument_dict_ccys(direction, ccy, 'Call', strike, date, stratid),
                    ]
                )

                stratid += 1

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


def get_instruments_put_fx(ccys, expiries, strikes, direction="Sell"):
    """

    """
    if type(strikes[0]) == tuple or type(strikes[0]) == list :
        raise KeyError("[-] Strikes should be a list of single values, not a list of tuples or lists")
    
    instruments = []
    stratid = 1
    
    # Create all instruments to price
    for date in expiries:
        
        for ccy in ccys:
            
            for strike in strikes:
                
                instruments.extend(
                    [
                        create_instrument_dict_ccys(direction, ccy, 'Put', strike, date, stratid)
                    ]
                )
                stratid += 1

    return instruments


def get_instruments_call_fx (ccys, expiries, strikes, direction="Sell") :
    """
    
    """
    if type(strikes[0])==tuple or type(strikes[0])==list:
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    
    instruments = []
    stratid = 0

    for date in expiries :
    
        for ccy in ccys :

            for strike in strikes :

                instruments.extend(
                    [
                        create_instrument_dict_ccys(direction, ccy, 'Call', strike, date, stratid)
                    ]
                )
                stratid += 1

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


# ------------------- Equity instruments -------------------#

def create_json_instrument_bggticker (direction, bbg_ticker, opt_type, strike, expiry, settlement_date, strat_id, notional=1_000_000, notional_ccy="EUR") :
    """
    
    Args :
        - direction : str ->
        - bbg_ticker : ->
        - opt_type : str -> Opertion type, it could be "Put" or "Call"
        - strike : ->
        - date : ->
        - strat_id : -> 
        - notional : int -> By default 1_000_000
        - notional_ccy : str -> Notional concurrecy by default "EUR"

    Returns :
        - instrument : dict -> Json instrument
    """
    instrument = {

        'direction' : direction,
        'BBGTicker' : bbg_ticker,
        'opt_type' : opt_type,
        'strike' : strike,
        'notional' : notional,
        'notional_currency' : notional_ccy,
        'expiry' : expiry,
        "stratid" : strat_id,
        "SettlementDate" : settlement_date
    }

    return instrument


def get_instruments_samestrike_sell_put_call_eq (BBG_tickers, expiries, strikes, direction="Sell") :
    """
    
    """
    if type(strikes[0])==tuple or type(strikes[0])==list:
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :
    
        for bbg_ticker in BBG_tickers :
    
            for strike in strikes :
                
                instruments.extend(
                    [
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Put', strike, date, date, stratid),
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Call', strike, date, date, stratid)
                    ]
                )

                stratid += 1

    return instruments



def get_instruments_2_strikes_sell_put_call_eq(BBG_tickers, expiries, strikes, direction="Sell"):
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
    
        for bbg_ticker in BBG_tickers :
    
            for strike in strikes :
    
                s1 = strike[0]
                s2 = strike[1]
    
                instruments.extend(
                    [
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Put', s1, date, date, stratid),
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Call', s2, date, date, stratid)
                    ]
                )

                stratid += 1

    return instruments


def get_instruments_put_eq (BBG_tickers, expiries, strikes, direction="Sell") :
    """
    
    """
    if type(strikes[0]) == tuple or type(strikes[0]) == list :
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for bbg_ticker in BBG_tickers :

            for strike in strikes :

                instruments.extend(
                    [
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Put', strike, date, date, stratid)
                    ]
                )
                stratid += 1

    return instruments


def get_instruments_call_eq(BBG_tickers, expiries, strikes, direction="Sell"):
    """
    
    """
    if type(strikes[0]) == tuple or type(strikes[0]) == list :
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    
    instruments = []
    stratid = 0

    # Create all instruments to price
    for date in expiries :

        for bbg_ticker in BBG_tickers :

            for strike in strikes :

                instruments.extend(
                    [
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Call', strike, date, date, stratid),
                    ]
                )

                stratid += 1

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
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Call', s1, date, date, stratid),
                        create_json_instrument_bggticker('Buy' if direction=="Sell" else "Sell", bbg_ticker, 'Call', s2, date, date, stratid)
                    ]
                )

                stratid += 1

    return instruments


def get_instruments_sell_ps_eq (BBG_tickers, expiries, strikes, direction="Sell") :
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
                        create_json_instrument_bggticker('Buy' if direction=="Sell" else "Sell", bbg_ticker, 'Put', s1, date, date, stratid),
                        create_json_instrument_bggticker(direction, bbg_ticker, 'Put', s2, date, date, stratid)
                    ]
                )
                
                stratid += 1

    return instruments