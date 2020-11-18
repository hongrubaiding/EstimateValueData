# -- coding: utf-8 --

'''
获取月报指数表格数据
'''

import pandas as pd
from GetAndSaveWindData.MysqlCon import MysqlCon
from GetAndSaveWindData.GetDataTotalMain import GetDataTotalMain

import numpy as np
from datetime import datetime, timedelta
import os
import matplotlib
import matplotlib.pyplot as plt
import statsmodels.api as sm
from WindPy import w

w.start()


class GetTableData:
    def __init__(self):
        dic_total_param = {}
        dic_param = {}
        dic_param['近1月'] = {'start_date': '2020-10-01', 'end_date': '2020-10-31'}
        dic_param['近3月'] = {'start_date': '2020-08-01', 'end_date': '2020-10-31'}
        dic_param['近6月'] = {'start_date': '2020-05-01', 'end_date': '2020-10-31'}
        dic_param['近1年'] = {'start_date': '2019-11-01', 'end_date': '2020-10-31'}
        dic_param['近3年'] = {'start_date': '2017-11-01', 'end_date': '2020-10-31'}
        dic_param['今年以来'] = {'start_date': '2020-01-01', 'end_date': '2020-10-31'}
        dic_total_param['区间'] = dic_param
        dic_total_param['年度其他'] = {'start_date': '2019-11-01', 'end_date': '2020-10-31'}
        dic_total_param['上月'] = {'start_date': '2020-09-01', 'end_date': '2020-09-30'}
        self.dic_total_param = dic_total_param


    def get_data(self, code_list=[], end_date='2020-09-30',index_type='宽基',pe_df=pd.DataFrame()):
        #获取证券名称
        wss_name_data = w.wss(codes=code_list, fields=['sec_name', ])
        name_df = pd.DataFrame(wss_name_data.Data, index=wss_name_data.Fields, columns=wss_name_data.Codes).T
        name_df.rename(columns={'sec_name'.upper(): '证券简称'}, inplace=True)

        #获取区间涨跌
        df_list = []
        for param_name, param_dic in self.dic_total_param['区间'].items():
            startDate = ('').join(param_dic['start_date'].split('-'))
            endDate = ('').join(param_dic['end_date'].split('-'))
            options = "startDate=%s;endDate=%s" % (startDate, endDate)
            wss_data = w.wss(codes=code_list, fields=['pct_chg_per', ], options=options)
            if wss_data.ErrorCode != 0:
                print("wind获区间涨跌数据有误，错误代码" + str(wss_data.ErrorCode))
                return pd.DataFrame()
            resultDf = pd.DataFrame(wss_data.Data, index=wss_data.Fields, columns=wss_data.Codes).T
            resultDf.rename(columns={'pct_chg_per'.upper(): param_name+'(%)', }, inplace=True)
            df_list.append(resultDf)
        total_data_df = pd.concat(df_list, axis=1, sort=True)
        total_df = pd.concat([total_data_df, name_df], axis=1, sort=True)

        #获取年度回撤，sharp，收益，波动等
        startDate = ('').join(self.dic_total_param['年度其他']['start_date'].split('-'))
        endDate = ('').join(self.dic_total_param['年度其他']['end_date'].split('-'))
        options = "startDate=%s;endDate=%s;period=2;returnType=1;riskFreeRate=1" % (startDate, endDate)
        wss_data = w.wss(codes=code_list,
                         fields=['risk_maxdownside', 'risk_sharpe', 'risk_stdevyearly', 'risk_returnyearly_index',],
                         options=options)
        if wss_data.ErrorCode != 0:
            print("wind获年度其他数据有误，错误代码" + str(wss_data.ErrorCode))
            return pd.DataFrame()
        risk_index_df = pd.DataFrame(wss_data.Data, index=wss_data.Fields, columns=wss_data.Codes).T
        risk_index_df.rename(columns={'risk_maxdownside'.upper(): '近一年最大回撤(%)', 'risk_sharpe'.upper(): 'Sharp比率',
                                      'risk_stdevyearly'.upper(): '年化波动(%)', 'risk_returnyearly_index'.upper(): '年化收益(%)'},inplace=True)
        total_final_df = pd.concat([total_df,risk_index_df],axis=1,sort=True)

        #获取月度成交额，换手率变化
        startDate = ('').join(self.dic_total_param['区间']['近1月']['start_date'].split('-'))
        endDate = ('').join(self.dic_total_param['区间']['近1月']['end_date'].split('-'))
        options = "unit=1;startDate=%s;endDate=%s" % (startDate, endDate)

        last_startDate = ('').join(self.dic_total_param['上月']['start_date'].split('-'))
        last_endDate = ('').join(self.dic_total_param['上月']['end_date'].split('-'))
        last_options = "unit=1;startDate=%s;endDate=%s" %(last_startDate,last_endDate)
        this_month = w.wss(codes=code_list, fields=["amt_per","turn_per"], options=options)
        this_month_df = pd.DataFrame(this_month.Data, index=this_month.Fields, columns=this_month.Codes).T

        last_month = w.wss(codes=code_list, fields=["amt_per","turn_per"], options=last_options)
        last_month_df = pd.DataFrame(last_month.Data, index=last_month.Fields, columns=last_month.Codes).T
        change_df = (this_month_df/last_month_df-1)*100
        change_df.rename(columns={'amt_per'.upper():'月度成交额变化(%)',"turn_per".upper():"月度换手率变化(%)"},inplace=True)
        total_last_df = pd.concat([total_final_df,change_df],axis=1,sort=True)
        total_last_df['证券代码'] = total_last_df.index
        return total_last_df



if __name__ == '__main__':
    GetTableDataDemo = GetTableData()
    GetTableDataDemo.get_data(code_list=['000016.SH', '000300.SH'])
