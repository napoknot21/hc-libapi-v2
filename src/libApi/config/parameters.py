import os
from dotenv import load_dotenv

load_dotenv()

# GLOBAL INFORMATION VARIABLES ICE API

ICE_HOST=os.getenv("ICE_HOST")
ICE_AUTH=os.getenv("ICE_AUTH")


ICE_USERNAME=os.getenv("ICE_USERNAME")
ICE_PASSWORD=os.getenv("ICE_PASSWORD")


ICE_URL_SEARCH_TRADES=os.getenv("ICE_URL_SEARCH_TRADES")
ICE_URL_GET_TRADES=os.getenv("ICE_URL_GET_TRADES")
ICE_URL_CALC_RES=os.getenv("ICE_URL_CALC_RES")
ICE_URL_BIL_IM_CALC=os.getenv("ICE_URL_BIL_IM_CALC")
ICE_URL_TRADE_ADD=os.getenv("ICE_URL_TRADE_ADD")
ICE_URL_INVOKE_CALC=os.getenv("ICE_URL_INVOKE_CALC")


ICE_URL_QUERY_RESULTS=os.getenv("ICE_URL_QUERY_RESULTS")
ICE_DATA_VS_ID=os.getenv("ICE_DATA_VS_ID")
ICE_URL_INVOKE_DQUERY=os.getenv("ICE_URL_INVOKE_DQUERY")
ICE_DATA_QUERY_ID=os.getenv("ICE_DATA_QUERY_ID")


FX_PRICER_SOLVE_PATH=os.getenv("FX_PRICER_SOLVE_PATH")

EQ_PRICER_CALC_PATH=os.getenv("EQ_PRICER_CALC_PATH")
EQ_PRICER_SOLVE_PATH=os.getenv("EQ_PRICER_SOLVE_PATH")

API_LOG_REQUEST_FILE_PATH=os.getenv("API_LOG_REQUEST_FILE_PATH")
FILE_ID_CALCULATION_PATH=os.getenv("FILE_ID_CALCULATION_PATH")
PRICING_LOG_FILE_PATH=os.getenv("PRICING_LOG_FILE_PATH")
SAVED_REQUESTS_DIRECTORY_PATH=os.getenv("SAVED_REQUESTS_DIRECTORY_PATH")


# instruments concurrencies
CCYS_ORDER=['EUR', 'USD', 'CHF', 'CAD', 'JPY', 'GBP', 'SEK', 'NOK']


# Book names
BOOK_NAMES=(os.getenv("BOOK_NAMES")).split(",")
BOOK_N_SUBBOOK_NAMES=(os.getenv("BOOK_N_SUBBOOK_NAMES")).split(",")

BOOK_NAME_HV=(os.getenv("BOOK_NAMES_HV")).split(",")
BOOK_NAME_WR=(os.getenv("BOOK_NAMES_WR")).split(",")


# Counterparties and their names
counterparties = [

    {
        "name" : os.getenv("NAME_COUNTER_GSI"),
        "email" : os.getenv("EMAIL_COUNTER_GSI"),
        "mailSubject" : os.getenv("MAIL_SUBJECT_1"),
        "mailBody" : os.getenv("MAIL_BODY_1"),
        "nameInIce" : os.getenv("NAME_ICE_GSI")
    },

        {
        "name" : os.getenv("NAME_COUNTER_MS"),
        "email" : os.getenv("EMAIL_COUNTER_MS"),
        "mailSubject" : os.getenv("MAIL_SUBJECT_2"),
        "mailBody" : os.getenv("MAIL_BODY_2"),
        "nameInIce" : os.getenv("NAME_ICE_MS")
    },

]


# Columns in the pricer
columnsInPricer = {

    "price" : "sum",
    "priceCurrency" : "first",
    "remainingNotional": "sum",
    "status" : "first",
    "IsFlippedResults" : "first",
    "MarketPriceAskPercentBase" : "sum",
    "MarketPriceAskPercentTerm" : "sum",
    "MarketPriceBase" : "sum",
    "MarketPriceBase_currency" : "first",
    "MarketPriceBaseAsk" : "sum",
    "MarketPriceBaseAsk_currency":"first",
    "MarketPriceBaseBid":"sum",
    "MarketPriceBaseBid_currency":"first",
    "MarketPriceBidPercentBase":"sum",
    "MarketPriceBidPercentTerm":"sum",
    "MarketPriceTerm":"sum",
    "MarketPriceTerm_currency":"first",
    "MarketPriceTermAsk":"sum",
    "MarketPriceTermAsk_currency":"first",
    "MarketPriceTermBid":"sum",
    "MarketPriceTermBid_currency":"first",
    "PricePerUnitPercentBase":"sum",
    "PricePerUnitPercentTerm":"sum",
    "ThetaBase":"sum",
    "ThetaBase_currency":"first",
    "ThetaBasePercent":"sum",
    "ThetaBasePercent_currency":"first",
    "ThetaPercent":"sum",
    "ThetaTerm":"sum",
    "ThetaTerm_currency":"first",
    "asset":"first",
    "25DButterfly":"first",
    "25DRiskReversal":"first",
    "AtmVolatility":"first",
    "DataSource":"first",
    "DeltaBase":"first",
    "DeltaBase_currency":"first",
    "DeltaBasePercent":"sum",
    "DeltaBasePremiumBase":"sum",
    "DeltaBasePremiumBase_currency":"first",
    "DeltaBasePremiumTerm":"sum",
    "DeltaBasePremiumTerm_currency":"first",
    "DeltaTerm":"sum",
    "DeltaTerm_currency":"first",
    "DeltaTermPercent":"sum",
    "DeltaTermPremiumBase":"sum",
    "DeltaTermPremiumBase_currency":"first",
    "DeltaTermPremiumTerm":"sum",
    "DeltaTermPremiumTerm_currency":"first",
    "DepoBase":"first",
    "DepoTerm":"first",
    "ForwardPoints":"first",
    "ForwardRate":"first",
    "GammaBase":"first",
    "GammaBase_currency":"first",
    "GammaBasePercent":"sum",
    "GammaPercent":"sum",
    "GammaTerm":"sum",
    "GammaTerm_currency":"first",
    "GammaTermPercent":"sum",
    "Spot":"first",
    "SpotSource":"first",
    "VegaBase":"sum",
    "VegaBase_currency":"first",
    "VegaPercent":"sum",
    "VegaTerm":"sum",
    "VegaTerm_currency":"first",
    "VolatilitySpread":"first",
    "direction":" ".join,
    "pair":"first",
    "opt_type":"first",
    "strike":"first",
    "notional":"first",
    "notional_currency":"first",
    "expiry":"first",
    "strike":" ".join,
    "CurrentNotional":" ".join,
    "CurrentNotional_currency":"first",
    "MarketValueAsk":"sum",
    "MarketValueAsk_currency":"first",
    "MarketValueBid":"sum",
    "MarketValueBid_currency":"first",
    "MarketValueMid":"sum",
    "MarketValueMid_currency":"first",
    "MarketValuePercent":"sum",
    "MarketValuePercentAsk":"sum",
    "MarketValuePercentBid":"sum",
    "MarketVol":"sum",
    "PricePerUnit":"sum",
    "PricePerUnit_currency":"first",
    "asset":"first",
    "BBGTicker":"first",

}