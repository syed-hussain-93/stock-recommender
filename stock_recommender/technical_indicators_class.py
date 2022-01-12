import pandas as pd
import ta
import numpy as np


class TechnicalIndicators:
    def __init__(self, data_frame: pd.DataFrame) -> None:

        self.df = data_frame

    def MACD_decision(self):

        self.df["MACD_diff"] = ta.trend.macd_diff(self.df["Close"])
        self.df["Decision_MACD"] = np.where(
            (self.df["MACD_diff"] > 0) & (self.df["MACD_diff"].shift(1) < 0),
            True,
            False,
        )

    def Goldencross_decision(
        self, short_term_window: int = 20, long_term_window: int = 50
    ) -> pd.DataFrame:
        self.df[f"SMA{short_term_window}"] = ta.trend.sma_indicator(
            self.df["Close"], window=short_term_window
        )
        self.df[f"SMA{long_term_window}"] = ta.trend.sma_indicator(
            self.df["Close"], window=long_term_window
        )

        self.df["Signal"] = np.where(
            self.df[f"SMA{short_term_window}"] > self.df[f"SMA{long_term_window}"],
            True,
            False,
        )
        self.df["Decision_GC"] = self.df["Signal"].diff()

    def RSI_SMA_decision(self, RSI_window: int = 10, SMA_window: int = 200):

        self.df[f"RSI{RSI_window}"] = ta.momentum.rsi(self.df["Close"], window=10)
        self.df[f"SMA{SMA_window}"] = ta.trend.sma_indicator(
            self.df["Close"], window=SMA_window
        )
        self.df[f"Decision_RSI/SMA"] = np.where(
            (self.df["Close"] > self.df[f"SMA{SMA_window}"])
            & (self.df[f"RSI{RSI_window}"] < 30),
            True,
            False,
        )

    def apply_technicals(self) -> pd.DataFrame:

        self.MACD_decision()
        self.Goldencross_decision()
        self.RSI_SMA_decision()

        return self.df
