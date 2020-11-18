# -- coding: utf-8 --

'''
    对本月的增强型指数基金状况统计，输出ppt中所用基金表格数据
'''

import pandas as pd
from datetime import datetime
import mylog as mylog
from GetAndSaveWindData.MysqlCon import MysqlCon
import numpy as np
from WindPy import w
import os
w.start()

class FundImproveEst:
    def __init__(self):
        self.file_path=r'D:\\工作文件\\'
        self.file_loc = r"D:\\工作文件\\指数基金月报\\202011\\"
        self.file_target =  r"D:\\工作文件\\指数基金月报\\202011\\基金ppt概况\\"
        self.name_se=[
            '基金代码', '基金简称', '基金成立日', '基金规模(亿元)', '基金管理人', '跟踪指数','近一月超额(%)', '近1月(%)', '近3月(%)', '近6月(%)', '近1年(%)', '近3年(%)',
            '今年以来(%)', '近一年最大回撤', '夏普比率', '年化波动', '年化收益', ]

    def get_data(self):
        total_improve_df = pd.read_excel(r'D:\\工作文件\\增强指数基金11月.xlsx')
        name_dic = {"fund_setupdate": "基金成立日", "netasset_total": "基金规模(亿元)", "fund_trackerror_threshold": "年化跟踪误差(%)",
                    "fund_corp_fundmanagementcompany": "基金管理人", "fund_trackindexcode": "跟踪指数代码",
                    "nav": "单位净值", "return_1m": "近1月(%)", "return_3m": "近3月(%)", "return_ytd": "今年以来(%)",
                    "return_1y": "近1年(%)", "risk_returnyearly": "年化收益", "risk_stdevyearly": "年化波动",
                    "sec_name": "基金简称", "return_6m": "近6月(%)", "return_3y": "近3年(%)", "risk_sharpe": "夏普比率",
                    "risk_maxdownside": "近一年最大回撤"}
        name_dic_reuslt = {key.upper(): values for key, values in name_dic.items()}

        total_code_list = total_improve_df['证券代码'].tolist()
        fields = "sec_name,fund_setupdate,netasset_total,fund_corp_fundmanagementcompany,fund_trackindexcode," \
                 "return_1m,return_3m,return_6m,return_1y,return_3y,return_ytd,risk_sharpe,risk_maxdownside,risk_returnyearly,risk_stdevyearly"
        options_str = "unit=1;tradeDate=20201101;annualized=0;startDate=20191031;endDate=20201031;period=2;returnType=1;yield=1;riskFreeRate=1"
        wssdata = w.wss(codes=total_code_list, fields=fields, options=options_str)
        if wssdata.ErrorCode != 0:
            print("获取wind数据错误%s" % wssdata.ErrorCode)
            return
        resultDf = pd.DataFrame(wssdata.Data, index=wssdata.Fields, columns=wssdata.Codes).T
        resultDf.index.name = '基金代码'
        resultDf.rename(columns=name_dic_reuslt, inplace=True)
        resultDf['基金规模(亿元)'] = resultDf['基金规模(亿元)'] / 100000000
        resultDf.sort_values(by='基金规模(亿元)', ascending=False, inplace=True)
        resultDf.to_excel(self.file_loc + '11月增强型指数基金表现.xlsx')
        return resultDf

    def calc_detail_df(self,temp_df):
        bench_code_list = list(temp_df['跟踪指数代码'].unique())
        options_str = "startDate=20201001;endDate=20201031"
        wssdata = w.wss(codes=bench_code_list, fields=['pct_chg_per','sec_name'], options=options_str)
        if wssdata.ErrorCode != 0:
            print("获取wind数据错误%s" % wssdata.ErrorCode)
            return
        bench_return_df = pd.DataFrame(wssdata.Data, index=wssdata.Fields, columns=wssdata.Codes).T
        bench_return_df.rename(columns={'pct_chg_per'.upper():"跟踪指数近1月",'sec_name'.upper():"证券简称"},inplace=True)
        df_other_list= []
        for index_code,df in temp_df.groupby(by='跟踪指数代码'):
            if index_code=='000300.SH':
                df = df[df['基金规模(亿元)']>=2]
                save_str = '沪深300'
            elif index_code=='000905.SH':
                df = df[df['基金规模(亿元)'] >= 2]
                save_str = '中证500'
            else:
                df = df[df['基金规模(亿元)'] >= 2]
            index_code = index_code.upper()
            df['近一月超额(%)'] = df['近1月(%)'] - bench_return_df.loc[index_code]['跟踪指数近1月']
            df['跟踪指数'] = bench_return_df.loc[index_code]['证券简称']
            temp_final_df = df.sort_values(by='近1月(%)').drop('跟踪指数代码',axis=1)
            temp_final_df['基金代码']= temp_final_df.index
            temp_final_df = temp_final_df[self.name_se]
            if index_code in ['000300.SH','000905.SH']:
                temp_final_df.to_excel(self.file_target+"增强产品%s概况.xlsx"%save_str,index=False)
            else:
                df_other_list.append(temp_final_df)

        if df_other_list:
            total_other_df = pd.concat(df_other_list,axis=0,sort=True)
            total_other_df = total_other_df[self.name_se]
            total_other_df.to_excel(self.file_target + "增强产品其他指数概况.xlsx",index=False)


    def get_main(self):
        total_df = self.get_data()
        self.calc_detail_df(total_df)

if __name__=='__main__':
    FundImproveEstDemo = FundImproveEst()
    FundImproveEstDemo.get_main()