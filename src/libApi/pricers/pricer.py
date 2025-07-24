import pandas as pd
from datetime import datetime

from libApi.ice.trade_manager import TradeManager

PRINCING_LOG_FILE="" # from PARAMETERS


class Pricer :

    def __init__ (self) :
        self.api = TradeManager()

    
    def log_api_call (self, n_instruments) -> None :
        """
        
        """
        logs = pd.read_csv(PRINCING_LOG_FILE)
        logs = logs._append(
            {
                "date" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "n_instruments" : n_instruments
            },
            ignore_index=True
        )

        logs.to_csv(PRINCING_LOG_FILE, index=False)