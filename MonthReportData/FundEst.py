# -- coding: utf-8 --

'''
    对本月的基金状况统计，输出ppt中所用基金表格数据
'''

import pandas as pd
from datetime import datetime
import mylog as mylog
from GetAndSaveWindData.MysqlCon import MysqlCon
import numpy as np
from WindPy import w
import os


class FundEst:
    def __init__(self):
        self.file_loc = r"D:\\工作文件\\指数基金月报\\202011\\基金概况\\"
        self.ppt_loc = r"D:\\工作文件\\指数基金月报\\202011\\基金ppt概况\\"
        self.name_se = [
            '基金代码', '基金简称', '基金成立日', '基金规模(亿元)', '基金管理人', '跟踪指数', '近1月(%)', '近3月(%)', '近6月(%)', '近1年(%)', '近3年(%)',
            '今年以来(%)', '近一年最大回撤', '夏普比率', '年化波动', '年化收益', ]

    def get_data(self):
        total_fund_file = os.listdir(self.file_loc)
        dic_df = {}
        for fund_file_name in total_fund_file:
            temp_df = pd.read_excel(self.file_loc + fund_file_name, index_col=0)
            temp_df['基金代码'] = temp_df.index
            dic_df[fund_file_name[:-5]] = temp_df
        return dic_df

    def calc_size_fund(self, temp_df):
        Hs300_df = temp_df[temp_df['跟踪指数代码'] == '000300.SH']
        temp_hs300_df = Hs300_df.sort_values(by='基金规模(亿元)', ascending=False, ).iloc[:25].sort_values(
            by='近1月(%)', ascending=False).drop('跟踪指数代码', axis=1)
        temp_hs300_df['跟踪指数'] = '沪深300'
        temp_hs300_df = temp_hs300_df[self.name_se]
        temp_hs300_df.to_excel(self.ppt_loc + "跟踪沪深300概况.xlsx", index=False)

        Zz500_df = temp_df[temp_df['跟踪指数代码'] == '000905.SH']
        temp_zz500_df = Zz500_df.sort_values(by='基金规模(亿元)', ascending=False, ).iloc[:15].sort_values(
            by='近1月(%)', ascending=False).drop('跟踪指数代码', axis=1)
        temp_zz500_df['跟踪指数'] = '中证500'
        temp_zz500_df = temp_zz500_df[self.name_se]
        temp_zz500_df.to_excel(self.ppt_loc + "跟踪中证500概况.xlsx", index=False)

        Sz50_df = temp_df[temp_df['跟踪指数代码'] == '000016.SH']
        temp_sz50_df = Sz50_df.sort_values(by='基金规模(亿元)', ascending=False, ).iloc[:3].sort_values(
            by='近1月(%)', ascending=False).drop('跟踪指数代码', axis=1)
        temp_sz50_df['跟踪指数'] = '上证50'
        temp_sz50_df = temp_sz50_df[self.name_se]
        temp_sz50_df.to_excel(self.ppt_loc + "跟踪上证50概况.xlsx", index=False)

        Cybz_df = temp_df[temp_df['跟踪指数代码'] == '399006.SZ']
        temp_cybz_df = Cybz_df.sort_values(by='基金规模(亿元)', ascending=False, ).iloc[:3].sort_values(
            by='近1月(%)', ascending=False).drop('跟踪指数代码', axis=1)
        temp_cybz_df['跟踪指数'] = '创业板指'
        temp_cybz_df = temp_cybz_df[self.name_se]
        temp_cybz_df.to_excel(self.ppt_loc + "跟踪创业板指概况.xlsx", index=False)

        other_code_dic = {'399330.SZ': '深证100', '000906.SH': '中证800', '000903.SH': "中证100", "399001.SZ": "深证成指",
                          "000010.SH": "上证180", "000001.SH": "上证指数", "399005.SZ": "中小板指","000688.SH":"科创50"}
        df_list=[]
        for index_code, other_df in temp_df.groupby(by='跟踪指数代码'):
            target_df = other_df[other_df['基金规模(亿元)'] >= 3]
            if index_code in other_code_dic and not target_df.empty:
                temp_indexcode_df = target_df.sort_values(by='基金规模(亿元)', ascending=False, ).sort_values(
                    by='近1月(%)', ascending=False).drop('跟踪指数代码', axis=1)
                temp_indexcode_df['跟踪指数'] = other_code_dic[index_code]
                df_list.append(temp_indexcode_df)

        if df_list:
            other_target_df = pd.concat(df_list,axis=0,sort=True)
            other_target_df = other_target_df[self.name_se]
            other_target_df.to_excel(self.ppt_loc + "跟踪其他指数概况.xlsx", index=False)

    def calc_topic_fund(self,temp_df):
        temp_topic_df =temp_df[temp_df['基金规模(亿元)'] >= 20]
        temp_topic_df.sort_values(by='基金规模(亿元)',inplace=True)

        df_list=[]
        for index_code,df in temp_topic_df.groupby(by='跟踪指数代码'):
            df_list.append(df.iloc[:1])
        total_df = pd.concat(df_list,axis=0,sort=True)
        total_df.sort_values(by='近1月(%)', ascending=False,inplace=True)
        total_df.rename(columns={"跟踪指数代码":"跟踪指数"},inplace=True)
        total_df = total_df[self.name_se]
        total_df.to_excel(self.ppt_loc + "跟踪主题指数概况.xlsx", index=False)

    def calc_indus_fund(self,temp_df):
        temp_indus_df = temp_df[temp_df['基金规模(亿元)'] >= 10]
        temp_indus_df.sort_values(by='基金规模(亿元)', inplace=True)

        df_list = []
        for index_code, df in temp_indus_df.groupby(by='跟踪指数代码'):
            df_list.append(df)
        total_df = pd.concat(df_list, axis=0, sort=True)
        total_df.rename(columns={"跟踪指数代码": "跟踪指数"}, inplace=True)
        total_df = total_df[self.name_se]
        total_df.to_excel(self.ppt_loc + "跟踪行业指数概况.xlsx", index=False)

    def calc_strate_fund(self,temp_df):
        temp_strate_df = temp_df[temp_df['基金规模(亿元)'] >= 5]
        temp_strate_df.sort_values(by='基金规模(亿元)', inplace=True)

        df_list = []
        for index_code, df in temp_strate_df.groupby(by='跟踪指数代码'):
            df_list.append(df)
        total_df = pd.concat(df_list, axis=0, sort=True)
        total_df.rename(columns={"跟踪指数代码": "跟踪指数"}, inplace=True)
        total_df = total_df[self.name_se]
        total_df.to_excel(self.ppt_loc + "跟踪策略指数概况.xlsx", index=False)

    def calc_style_fund(self,temp_df):
        temp_style_df = temp_df.sort_values(by='基金规模(亿元)')

        df_list = []
        for index_code, df in temp_style_df.groupby(by='跟踪指数代码'):
            df_list.append(df)
        total_df = pd.concat(df_list, axis=0, sort=True)
        total_df.rename(columns={"跟踪指数代码": "跟踪指数"}, inplace=True)
        total_df = total_df[self.name_se]
        total_df.to_excel(self.ppt_loc + "跟踪风格指数概况.xlsx", index=False)

    def get_main(self):
        dic_df = self.get_data()
        for file_name, temp_df in dic_df.items():
            if file_name.find('规模') != -1:
                self.calc_size_fund(temp_df)
            elif file_name.find('主题')!=-1:
                self.calc_topic_fund(temp_df)
            elif file_name.find('行业')!=-1:
                self.calc_indus_fund(temp_df)
            elif file_name.find('策略')!=-1:
                self.calc_strate_fund(temp_df)
            elif file_name.find('风格')!=-1:
                self.calc_style_fund(temp_df)


if __name__ == '__main__':
    FundEstDemo = FundEst()
    FundEstDemo.get_main()
