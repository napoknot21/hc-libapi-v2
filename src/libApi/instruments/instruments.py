CCYS_ORDER=['EUR', 'USD', 'CHF', 'CAD', 'JPY', 'GBP', 'SEK', 'NOK']


def find_ccy (ccy : str) :
    """
    
    """
    for i in range(len(CCYS_ORDER)):
    
        if CCYS_ORDER[i] in ccy:
            return CCYS_ORDER[i]
    
    return None


# ------------------- FX instruments -------------------#

def get_instruments_samestrike_sell_put_call_fx (ccys, expiries, strikes, direction="Sell") :
    """
    
    """
    if type(strikes[0]) == tuple or type(strikes[0]) == list :
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    
    instruments = []
    stratid=0

    # Create all instruments to price
    for date in expiries :

        for ccy in ccys :
            
            for strike in strikes :
                
                instruments.extend(
                    [
                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type' : 'Put',
                            'strike' : strike,
                            'notional' : 1000000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" : stratid
                        },

                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type' : 'Call',
                            'strike' : strike,
                            'notional' : 1000000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" : stratid
                        },
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
                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type' : 'Put',
                            'strike' : s1, 'notional' : 1000000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" :stratid
                        },

                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type' : 'Call',
                            'strike': s2,
                            'notional' : 1000000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date, "stratid" :stratid
                        }
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
    stratid=1
    
    # Create all instruments to price
    for date in expiries:
        
        for ccy in ccys:
            
            for strike in strikes:
                
                instruments.extend(
                    [
                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type': 'Put',
                            'strike': strike,
                            'notional': 1000000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" : stratid
                        }
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
                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type' : 'Call',
                            'strike' : strike,
                            'notional' : 1000000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" : stratid
                        }
                    ]
                )
                stratid += 1

    return instruments


def get_instruments_sell_cs_fx (ccys, expiries, strikes, direction="Sell") :
    """

    """
    if type(strikes[0]) != tuple and type(strikes[0]) != list :
        # 
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
                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type' : 'Call',
                            'strike' : s1,
                            'notional' : 1_000_000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" : stratid
                        },
                        
                        {
                            'direction' : 'Buy' if direction == "Sell" else "Sell",
                            'pair' : ccy,
                            'opt_type' : 'Call',
                            'strike' : s2,
                            'notional' : 1_000_000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid":stratid
                        }
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
                        {
                            'direction' : 'Buy' if direction == "Sell" else "Sell",
                            'pair' : ccy,
                            'opt_type' : 'Put',
                            'strike' : s1,
                            'notional' : 1_000_000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" : stratid
                        },
                        
                        {
                            'direction' : direction,
                            'pair' : ccy,
                            'opt_type' : 'Put',
                            'strike' : s2,
                            'notional' : 1_000_000,
                            'notional_currency' : find_ccy(ccy),
                            'expiry' : date,
                            "stratid" : stratid
                        }
                    ]
                )
                stratid += 1

    return instruments


# ------------------- Equity instruments -------------------#

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
                        {
                            'direction' : direction,
                            'BBGTicker' : bbg_ticker,
                            'opt_type' : 'Put',
                            'strike' : strike,
                            'notional' : 1_000_000,
                            'notional_currency' : "EUR",
                            'expiry' : date,
                            "stratid" : stratid,
                            "SettlementDate" : date
                        },
                        
                        {
                            'direction' : direction,
                            'BBGTicker' : bbg_ticker,
                            'opt_type' : 'Call',
                            'strike' : strike,
                            'notional' : 1_000_000,
                            'notional_currency': "EUR",
                            'expiry' : date,
                            "stratid" : stratid,
                            "SettlementDate" : date
                        },
                    ]
                )
                stratid += 1

    return instruments


def get_instruments_2_strikes_sell_put_call_eq(BBG_tickers, expiries, strikes, direction="Sell"):
    """Create instruments for a strangle strategy
    here strikes is a list of tuples, each tuple contains two strikes, one for the put and one for the call
    """
    if type(strikes[0])!=tuple and type(strikes[0])!=list:
        raise KeyError("Strikes should be a list of tuples or lists, not a list of single values")
    instruments = []
    stratid=0
    # Create all instruments to price
    for date in expiries:
        for bbg_ticker in BBG_tickers:
            for strike in strikes:
                s1 = strike[0]
                s2 = strike[1]
                instruments.extend([
                        {'direction': direction, 'BBGTicker': bbg_ticker, 'opt_type': 'Put', 'strike': s1, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date},
                        {'direction': direction, 'BBGTicker': bbg_ticker, 'opt_type': 'Call', 'strike': s2, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date}
                ])
                stratid += 1
    return instruments

def get_instruments_put_eq(BBG_tickers, expiries, strikes, direction="Sell"):
    if type(strikes[0])==tuple or type(strikes[0])==list:
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    instruments = []
    stratid=0
    # Create all instruments to price
    for date in expiries:
        for bbg_ticker in BBG_tickers:
            for strike in strikes:
                instruments.extend([
                        {'direction': direction, 'BBGTicker': bbg_ticker, 'opt_type': 'Put', 'strike': strike, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date}
                ])
                stratid += 1
    return instruments

def get_instruments_call_eq(BBG_tickers, expiries, strikes, direction="Sell"):
    if type(strikes[0])==tuple or type(strikes[0])==list:
        raise KeyError("Strikes should be a list of single values, not a list of tuples or lists")
    instruments = []
    stratid=0
    for date in expiries:
        for bbg_ticker in BBG_tickers:
            for strike in strikes:
                instruments.extend([
                        {'direction': direction, 'BBGTicker': bbg_ticker, 'opt_type': 'Call', 'strike': strike, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date}
                ])
                stratid += 1
    return instruments

def get_instruments_sell_cs_eq(BBG_tickers, expiries, strikes, direction="Sell"):
    if type(strikes[0])!=tuple and type(strikes[0])!=list:
        raise KeyError("Strikes should be a list of tuples or lists, not a list of single values")
    instruments = []
    stratid=0
    # Create all instruments to price
    for date in expiries:
        for bbg_ticker in BBG_tickers:
            for strike in strikes:
                s1 = strike[0]
                s2 = strike[1]
                instruments.extend([
                        {'direction': direction, 'BBGTicker': bbg_ticker, 'opt_type': 'Call', 'strike': s1, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date},
                        {'direction': 'Buy' if direction=="Sell" else "Sell", 'BBGTicker': bbg_ticker, 'opt_type': 'Call', 'strike': s2, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date}
                ])
                stratid += 1
    return instruments

def get_instruments_sell_ps_eq(BBG_tickers, expiries, strikes, direction="Sell"):
    if type(strikes[0])!=tuple and type(strikes[0])!=list:
        raise KeyError("Strikes should be a list of tuples or lists, not a list of single values")
    instruments = []
    stratid=0
    # Create all instruments to price
    for date in expiries:
        for bbg_ticker in BBG_tickers:
            for strike in strikes:
                s1 = strike[0]
                s2 = strike[1]
                instruments.extend([
                        {'direction': 'Buy' if direction=="Sell" else "Sell", 'BBGTicker': bbg_ticker, 'opt_type': 'Put', 'strike': s1, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date},
                        {'direction': direction, 'BBGTicker': bbg_ticker, 'opt_type': 'Put', 'strike': s2, 'notional': 1000000, 'notional_currency': "EUR", 'expiry': date, "stratid":stratid, "SettlementDate":date}
                ])
                stratid += 1
    return instruments