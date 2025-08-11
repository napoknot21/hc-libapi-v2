from __future__ import annotations
import os
from dotenv import load_dotenv, find_dotenv

# If the .env sits next to this file:
ENV_PATH = find_dotenv()

if ENV_PATH :
    load_dotenv(ENV_PATH)


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
API_LOG_REQUEST_FILE_CSV_PATH=os.getenv("API_LOG_REQUEST_FILE_CSV_PATH")

FILE_ID_CALCULATION_PATH=os.getenv("FILE_ID_CALCULATION_PATH")
FILE_ID_CALCULATION_CSV_PATH=os.getenv("FILE_ID_CALCULATION_CSV_PATH")

PRICING_LOG_FILE_PATH=os.getenv("PRICING_LOG_FILE_PATH")

SAVED_REQUESTS_DIRECTORY_PATH=os.getenv("SAVED_REQUESTS_DIRECTORY_PATH")


BOOK_NAMES_HV_ALL=os.getenv("BOOK_NAMES_HV_ALL")
BOOK_NAMES_WR_ALL=os.getenv("BOOK_NAMES_WR_ALL")

BOOK_NAMES_HV_SUBSET_N1=os.getenv("BOOK_NAMES_HV_SUBSET_N1")
BOOK_NAMES_HV_SUBSET_N2=os.getenv("BOOK_NAMES_HV_SUBSET_N2")


# Optional parameters (to avoid to convert every time the list)

BOOK_NAMES_HV_LIST_ALL=list(BOOK_NAMES_HV_ALL.split(","))
BOOK_NAMES_WR_LIST_ALL=list(BOOK_NAMES_WR_ALL.split(","))

BOOK_NAMES_HV_LIST_SUBSET_N1=list(BOOK_NAMES_HV_SUBSET_N1.split(","))
BOOK_NAMES_HV_LIST_SUBSET_N2=list(BOOK_NAMES_HV_SUBSET_N2.split(","))


# instruments concurrencies
CCYS_ORDER=['EUR', 'USD', 'CHF', 'CAD', 'JPY', 'GBP', 'SEK', 'NOK']


COUNTERPARTIES=[

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
COLUMNS_IN_PRICER={

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
    "pair" : "first",
    "opt_type" : "first",
    "strike" : "first",
    "notional" : "first",
    "notional_currency" : "first",
    "expiry" : "first",
    "strike" : " ".join,
    "CurrentNotional": " ".join,
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