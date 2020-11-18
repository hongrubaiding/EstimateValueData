# -- coding: utf-8 --
import pandas as pd
import numpy as np

from GetAndSaveWindData.GetDataTotalMain import GetDataTotalMain


class StockFactorCalcl:
    def __init__(self):
        self.GEtDataTotaMainDemo = GetDataTotalMain(data_resource='wind')


    def get_main(self):
        pass


    def get_history_data(self):
        pass

    def get_wash_data(self):
        pass


if __name__=='_main__':
    StockFactorCalclDemo = StockFactorCalcl()
    StockFactorCalclDemo.get_main()