# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

import pandas as pd
import numpy as np
import mylog as mylog
from GetAndSaveWindData.GetDataTotalMain import GetDataTotalMain
import matplotlib.pyplot as plt
import matplotlib
from datetime import datetime,timedelta

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['axes.unicode_minus'] = False


class GetFinanceReportData:
    def __init__(self, dic_param, file_path=''):
        self.dic_param = dic_param
        self.GetDataTotalMainDemo = GetDataTotalMain(data_resource='wind')
        self.file_path=file_path

    def get_industry_sta(self,dic_df):
        '''
        基金持股行业统计
        :param dic_df:
        :return:
        '''
        dic_stock_weight = {}       #占股票投资市值比
        dic_net_value_weight={}     #占净值比
        for rpt_date,temp_df in dic_df.items():
            total_code_list = temp_df['stock_code'].tolist()
            if rpt_date.find('中报')!=-1:
                rpt_date_str = rpt_date[:4]+'0630'
            else:
                rpt_date_str = rpt_date[:4]+'1231'
            temp_new_df = temp_df.set_index('stock_code')
            df = self.GetDataTotalMainDemo.get_stock_industry(industry_flag='中证',code_list=total_code_list,industryType=1,tradeDate=rpt_date_str)
            temp_total_df = pd.concat([temp_new_df,df],axis=1,sort=True)
            dic_stock_weight[rpt_date_str] = {}
            dic_net_value_weight[rpt_date_str] = {}
            for industry,stock_df in temp_total_df.groupby(df.columns.tolist()[0]):
                dic_stock_weight[rpt_date_str][industry]=stock_df['pro_total_stock_inve'].sum()
                dic_net_value_weight[rpt_date_str][industry] = stock_df['pro_net_value'].sum()
        stock_inves_rate_df = pd.DataFrame(dic_stock_weight).T.fillna(0)/100
        net_value_rate_df = pd.DataFrame(dic_net_value_weight).T.fillna(0)/100
        if self.file_path:
            stock_inves_rate_df.to_excel('占股票投资比例.xlsx')
            net_value_rate_df.to_excel("占净值比例.xlsx")

        fig1 = plt.figure(figsize=(16,9))
        ax_inves = fig1.add_subplot(111)
        stock_inves_rate_df.plot(kind='bar')
        plt.show()
        return stock_inves_rate_df,net_value_rate_df



    def get_main(self):

        fund_contain_stock_df = self.GetDataTotalMainDemo.get_fund_report_data(fund_code=dic_param['fund_code'],
                                                                          start_date=dic_param['start_date'],
                                                                          end_date=dic_param['end_date'])
        dic_df = {}
        total_rpt_list = fund_contain_stock_df.sort_values("rpt_date")['rpt_date'].tolist()
        for rpt_date,temp_df in fund_contain_stock_df.groupby(by='rpt_date'):
            dic_df[rpt_date] = temp_df
        self.get_industry_sta(dic_df)


if __name__ == '__main__':
    dic_param = {}
    dic_param['fund_code'] = '110022.OF'
    dic_param['fund_name'] = '富国上证综指'
    dic_param['start_date'] = '2011-01-30'
    dic_param['end_date'] = datetime.today().strftime("%Y-%m-%d")
    GetFinanceReportDataDemo = GetFinanceReportData(dic_param=dic_param)
    GetFinanceReportDataDemo.get_main()
