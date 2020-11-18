# -- coding: utf-8 --

'''
获取指数估值数据
'''

import pandas as pd
from GetAndSaveWindData.MysqlCon import MysqlCon
from GetAndSaveWindData.GetDataTotalMain import GetDataTotalMain
from datetime import datetime, timedelta
import os
from WindPy import w


class TalLiJudge:
    def __init__(self):
        self.GetDataTotalMainDemo = GetDataTotalMain(data_resource='wind')

    def get_data(self):
        target_df = pd.read_excel("基金发行明细.xlsx", sheet_name='Sheet1', )
        total_df = pd.read_excel("主动权益类基金.xlsx")
        dic_df = {}
        for name in target_df['基金经理'].tolist():
            name_list = name.split('、')
            if len(name_list) == 1:
                dic_df[name] = total_df[total_df['基金经理'] == name]
            else:
                for name in name_list:
                    temp_df_list = []
                    for target_name in total_df['基金经理'].tolist():
                        if target_name.find(name) != -1:
                            temp_df_list.append(total_df[total_df['基金经理'] == target_name])
                    temp_df = pd.concat(temp_df_list, axis=0, sort=True)
                    dic_df[name] = temp_df
        return dic_df

    def get_calc_result(self, dic_df):
        dic_name_df = {}
        dic_name_corr_df = {}
        dic_name_poc_df = {}
        for name, fund_df in dic_df.items():
            temp_df_list = []
            for num in range(fund_df.shape[0]):
                code = fund_df.iloc[num]['证券代码']
                se_name = fund_df.iloc[num]['证券简称']
                start_date = fund_df.iloc[num]['任职日期'].strftime("%Y-%m-%d")
                end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
                temp_df = self.GetDataTotalMainDemo.get_hq_data(code=code, start_date=start_date, end_date=end_date,
                                                                code_style='fund', name_list=['net_value_adj'])
                temp_df.rename(columns={'net_value_adj': se_name}, inplace=True)
                temp_return_df = temp_df / temp_df.shift(1) - 1
                temp_return_df.dropna(inplace=True)
                temp_df_list.append(temp_return_df)
            temp_total_df = pd.concat(temp_df_list, axis=1, sort=True)
            temp_total_df.dropna(axis=1, how='all', inplace=True)
            dic_name_df[name] = temp_total_df
            dic_name_corr_df[name] = temp_total_df.corr()
            fields = ["prt_stockvalue_topindustryname2", "prt_stockvalue_topindustrytonav2",
                      "prt_stockvalue_topindustrytostock2","sec_name"]
            name_dic = {"prt_stockvalue_topindustryname2".upper(): "重仓行业名称",
                        "prt_stockvalue_topindustrytonav2".upper(): "重仓行业市值占基金资产净值比",
                        "prt_stockvalue_topindustrytostock2".upper(): "重仓行业市值占股票投资市值比",
                        "sec_name".upper(): "证券简称"}
            poc_df_list = []
            for order in range(1,6):
                wss_data = w.wss(codes=fund_df['证券代码'].tolist(),fields=fields,options="rptDate=20200630;order=%s"%str(order))
                if wss_data.ErrorCode != 0:
                    print("wind获取因子数据有误，错误代码" + str(wss_data.ErrorCode))
                    continue
                resultDf = pd.DataFrame(wss_data.Data, index=wss_data.Fields, columns=wss_data.Codes).T
                resultDf.rename(columns=name_dic,inplace=True)
                resultDf['重仓行业排名']=order
                poc_df_list.append(resultDf)

            if poc_df_list:
                temp_total_poc = pd.concat(poc_df_list,axis=0,sort=True)
                dic_name_poc_df[name] = temp_total_poc
        save_path = os.getcwd() + '\\HXBFundManager\\基金经理管理产品相关性.xlsx'
        poc_save_path = os.getcwd() + '\\HXBFundManager\\基金经理重仓行业概况.xlsx'
        writer = pd.ExcelWriter(save_path)
        for fund_name, corr_df in dic_name_corr_df.items():
            corr_df.to_excel(writer, sheet_name=fund_name)
        writer.save()

        writer2 = pd.ExcelWriter(poc_save_path)
        for fund_name, poc_df in dic_name_poc_df.items():
            poc_df.to_excel(writer2, sheet_name=fund_name)
        writer2.save()

    def get_main(self):
        dic_df = self.get_data()
        self.get_calc_result(dic_df)


if __name__ == '__main__':
    TalLiJudgeDemo = TalLiJudge()
    TalLiJudgeDemo.get_main()
