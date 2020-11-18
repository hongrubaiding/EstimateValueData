# -- coding: utf-8 --

'''
    基金评价指标排名计算
'''

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


'''
证券代码	证券简称	基金成立日	'近1月(%)','近3月(%)','近6月(%)','近1年(%)','近3年(%)','基金规模(亿元)','年化收益率(%)	'
,'最大回撤(%)','年化波动率(%)','下行标准差(%)','Sharpe','Alpha(年化)(%)','Sharpe(年化)','Treynor(年化)','Sortino(年化)',
'选时能力','选股能力','信息比率(年化)','基金经理(现任)','基金管理人
'004840.OF','001708.OF','004695.OF','006749.OF','002465.OF','002182.OF','004696.OF','003208.OF'


'''


class CalcJudgeFund:
    def __init__(self):
        self.file_path = r"D:\\工作文件\\产品评价\\东兴\\"
        self.sort_up = ['近1月(%)', '近3月(%)', '近6月(%)', '近1年(%)', '近3年(%)', '基金规模(亿元)', '年化收益率(%)', '最大回撤(%)',
                        'Alpha(年化)(%)', 'Sharpe(年化)', 'Treynor(年化)', 'Sortino(年化)', '选时能力', '选股能力',
                        '信息比率(年化)']  # 倒序排名，越大越好
        self.sort_down =['年化波动率(%)','下行标准差(%)']         #逆序排名，越小越好
        self.targe_code_list=['004840.OF','001708.OF','004695.OF','006749.OF','002465.OF','002182.OF','004696.OF','003208.OF']

    def calc_sort(self):
        total_fund = pd.read_excel(self.file_path+"偏股混合型.xlsx", sheet_name='Sheet1', index_col=0)
        df_list = []
        for up_col in self.sort_up:
            temp_se = total_fund[up_col].rank(ascending=False)
            temp_dic = temp_se.to_dict()
            total_num  = temp_se.max()
            str_dic={}
            for fund_code,rank_num in temp_dic.items():
                if np.isnan(rank_num):
                    str_dic[fund_code] = '--/%s' % int(total_num)
                else:
                    str_dic[fund_code] = '%s/%s'%(int(rank_num),int(total_num))
            temp_new_se= pd.Series(str_dic,name=up_col)
            df_list.append(temp_new_se)

        for down_col in self.sort_down:
            temp_se = total_fund[down_col].rank(ascending=True)
            temp_dic = temp_se.to_dict()
            total_num = temp_se.max()
            str_dic = {}
            for fund_code, rank_num in temp_dic.items():
                if np.isnan(rank_num):
                    str_dic[fund_code] = '--/%s' % int(total_num)
                else:
                    str_dic[fund_code] = '%s/%s' % (int(rank_num), int(total_num))
            temp_new_se = pd.Series(str_dic, name=down_col)
            df_list.append(temp_new_se)
        sort_df = pd.concat(df_list,axis=1,sort=True)

        fix_df = total_fund[['证券简称','基金成立日','基金经理(现任)','基金管理人']]
        final_df = pd.concat([fix_df,sort_df],sort=True,axis=1)
        final_df.to_excel(self.file_path+"偏股混合型基金排名.xlsx")
        return final_df

    def get_target(self,df):
        target_df =df.loc[self.targe_code_list]


if __name__ == '__main__':
    CalcJudgeFundDemo = CalcJudgeFund()
    CalcJudgeFundDemo.calc_sort()
