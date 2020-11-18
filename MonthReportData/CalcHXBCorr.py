# -- coding: utf-8 --

'''
获取指数估值数据
'''

import pandas as pd
from GetAndSaveWindData.MysqlCon import MysqlCon
from GetAndSaveWindData.GetDataTotalMain import GetDataTotalMain
from datetime import datetime
import os


class CalcHXBCorr:
    def __init__(self):
        self.GetDataTotalMainDemo = GetDataTotalMain(data_resource='wind')

    def get_Data(self):
        pass

    def get_main(self):
        df1 = pd.read_excel("被动指数产品.xlsx", index_col=0)
        dic_size = {}
        for fund_ma,tempdf in df1.groupby(by='基金管理人'):
            dic_size[fund_ma] = tempdf['基金规模'].sum()
        size_se = pd.Series(dic_size,name='基金公司管理规模').sort_values(ascending=False)
        dic_save_df = {}
        for manage_name in size_se.index.tolist():
            dic_save_df[manage_name]=df1.loc[df1['基金管理人']==manage_name]

        save_path = os.getcwd() + '\\基金公司管理产品概况.xlsx'
        writer = pd.ExcelWriter(save_path)
        for fund_name, save_df in dic_save_df.items():
            save_df.to_excel(writer, sheet_name=fund_name)
        writer.save()


        df = pd.read_excel("基金发行明细.xlsx", sheet_name='Sheet1', index_col=0)
        dic_df = {}
        for code in df.index.tolist():
            start_date = df.loc[code]['起始日'].strftime("%Y-%m-%d")
            end_date = df.loc[code]['结尾日'].strftime("%Y-%m-%d")
            temp_df = self.GetDataTotalMainDemo.get_hq_data(code, code_style='fund', start_date=start_date,
                                                            end_date=end_date, name_list=['acc_net_value'])
            temp_df.rename(columns={"acc_net_value": code}, inplace=True)
            temp_return_df = temp_df / temp_df.shift(1) - 1
            temp_return_df.dropna(inplace=True)
            dic_df[df.loc[code]['名称']] = temp_return_df

        min_date = df['起始日'].min().strftime("%Y-%m-%d")
        max_date = df['结尾日'].max().strftime("%Y-%m-%d")
        code_list2 = ['000300.SH', '000905.SH', '000852.SH', '000935.SH', '000933.SH', '000932.SH', '000936.CSI',
                      '000934.SH', '000931.CSI', '000930.CSI','000929.CSI', '000937.CSI', '000928.SH']  #
        name_dic = {'000300.SH': '沪深300', '000905.SH': '中证500', '000852.SH': '中证1000', '000935.SH': '中证信息',
                    '000933.SH': '中证医药', '000932.SH': '中证消费', '000936.CSI': '中证电信','000934.SH': '中证金融',
                    '000930.CSI':'中证工业','000929.CSI':'中证材料','000937.CSI':'中证公用','000928.SH':'中证能源',
                    '000931.CSI':'中证可选'}

        index_df_list = []
        for code in code_list2:
            temp_df = self.GetDataTotalMainDemo.get_hq_data(code, code_style='index', start_date=min_date,
                                                            end_date=max_date, )
            temp_df.rename(columns={"close_price": code}, inplace=True)
            index_df_list.append(temp_df)
        index_df = pd.concat(index_df_list, axis=1, sort=True)
        index_df.dropna(inplace=True)
        index_return_df = index_df / index_df.shift(1) - 1
        index_return_df.rename(columns=name_dic,inplace=True)

        df_list=[]
        for fund_name, fund_df in dic_df.items():
            start_corr_date = fund_df.index.tolist()[0]
            end_corr_date = fund_df.index.tolist()[-1]
            temp_index_df = index_return_df.loc[
                (index_return_df.index >= start_corr_date) & (index_return_df.index <= end_corr_date)]
            fund_index_df = pd.concat([fund_df, temp_index_df], axis=1, sort=True)
            corr_df = fund_index_df.corr()
            temp_Se = corr_df.iloc[0][1:]
            temp_corr_df = pd.DataFrame(temp_Se.values,columns=[fund_name],index=temp_Se.index.tolist())
            df_list.append(temp_corr_df)
        final_df = pd.concat(df_list,axis=1,sort=True).T
        final_df.to_excel("基金相关系数.xlsx")


if __name__ == '__main__':
    CalcHXBCorrDemo = CalcHXBCorr()
    CalcHXBCorrDemo.get_main()
