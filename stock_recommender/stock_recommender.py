from sqlalchemy.engine.base import Engine
import pandas as pd

from typing import List

from technical_indicators_class import TechnicalIndicators
from database_setup_class import Database

class StockRecommender:
    
    def __init__(self, index_name: str, database: Database) -> None:
        self.index = index_name
        self.engine = database.engine
        self.database = database
    
    
    def clean_date(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        
        data_frame['Date'] = data_frame['Date'].apply(lambda x: x.split(' ')[0])
        return data_frame
    
    def get_tables(self) -> pd.DataFrame:
        query = f"SELECT name FROM sqlite_master WHERE type='table'"
        df = pd.read_sql(query, self.engine)

        return df
        
    def get_prices(self) -> List[pd.DataFrame]:
        prices = []
        for table in self.get_tables()['name']:
            query = f"SELECT date, close FROM `{table}`"
            prices.append(self.clean_date(pd.read_sql(query, self.engine)))
            
        return prices
    
    def read_data(self) -> List[pd.DataFrame]:
        
        data = [self.clean_date(pd.read_sql(
            f"SELECT * FROM `{table}`", self.engine
        )) for table in self.get_tables()['name']]
        
        return data
        
    
    def apply_technical_indicators(self, indicator:str = 'all') -> List[pd.DataFrame]:
        
        prices = self.get_prices()
        technical_indicators = [TechnicalIndicators(frame).apply_technicals() for frame in prices]

        return technical_indicators

    def recommender(self):
        indicator_names = ['Decision_MACD', 'Decision_GC', 'Decision_RSI/SMA']
        ticker_list = self.database.get_ticker_symbols()
        technical_indicators = self.apply_technical_indicators()
        for ticker_symbol, frame in zip(ticker_list, technical_indicators):
            if frame.empty is False:
                for indicator in indicator_names:
                    if frame[indicator].iloc[-1]:
                        print(f"{indicator} Buying signal for {ticker_symbol}")
            
        
