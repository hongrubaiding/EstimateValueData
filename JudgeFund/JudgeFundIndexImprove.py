# -- coding: utf-8 --

'''
    指数增强型基金评价
'''

import pandas as pd
import mylog as mylog
import numpy as np
from datetime import datetime, timedelta
import mylog as mylogdemo
from GetAndSaveWindData.GetDataTotalMain import GetDataTotalMain
import statsmodels.api as sm


class JudgeFundIndexImprove:
    def __init__(self):
        self.logger = mylogdemo.set_log()
        self.last_date_str = datetime.today().strftime("%Y-%m-%d")
        self.target_code = '110003.OF'

    def get_total_info(self, df, ):
        self.logger.info("截止当前最新日期%s,现有指数增强型基金(含A/C类)共%s只。" % (self.last_date_str, df.shape[0]))
        target_bench_code = df.loc[self.target_code]['跟踪指数代码']
        for bench_code, temp_df in df.groupby(by='跟踪指数代码'):
            if bench_code == target_bench_code:
                self.logger.info("其中跟踪指数为%s的增强型基金%s只" % (target_bench_code, temp_df.shape[0]))
                break

    def calc_select(self, dic_result_df):
        pass

    def get_data(self, df):
        '''
        获取跟踪指数和产品复权单位净值数据
        :param df:
        :return:
        '''
        dic_fund_index = {}
        for fund_code in df.index.tolist():
            if df.loc[fund_code]['证券简称'].find('C') == -1:
                dic_fund_index[fund_code] = df.loc[fund_code]['跟踪指数代码']

        GetDataTotalMainDemo = GetDataTotalMain(data_resource='wind')
        dic_total_index_df = {}
        dic_result_df = {}
        for fund_code, index_code in dic_fund_index.items():
            start_date = df.loc[fund_code]['基金成立日']
            if datetime.today() - timedelta(days=365) < start_date:
                continue
            start_date = start_date.strftime("%Y-%m-%d")
            temp_fund_df = GetDataTotalMainDemo.get_hq_data(code=fund_code, code_style='fund', start_date=start_date,
                                                            end_date=self.last_date_str, name_list=['net_value_adj'])
            temp_fund_df.rename(columns={'net_value_adj': fund_code}, inplace=True)
            if index_code not in dic_total_index_df:
                temp_index_df = GetDataTotalMainDemo.get_hq_data(code=index_code, code_style='index',
                                                                 start_date=start_date, end_date=self.last_date_str,
                                                                 name_list=['close_price'])
                temp_index_df.rename(columns={'close_price': index_code}, inplace=True)
                dic_total_index_df[index_code] = temp_index_df
            else:
                temp_index_df = dic_total_index_df[index_code]
            dic_result_df[fund_code + '_' + index_code] = pd.concat([temp_fund_df, temp_index_df], axis=1, sort=True)
        return dic_result_df

    def regression(self, x1, x2, y):
        '''
        最小二乘回归
        :param x1:
        :param x2:
        :param y:
        :return:
        '''
        x1, x2 = x1.reshape(len(x1), 1), x2.reshape(len(x2), 1)
        c = np.ones((len(x1), 1))
        X = np.hstack((c, x1, x2))
        res = (sm.OLS(y, X)).fit()
        return res

    def get_select_judge(self, df):
        '''
        获取所有基金的选股，择时能力
        :param df:
        :return:
        '''
        try:
            select_df = pd.read_excel("择时选股能力.xlsx", index_col=0)
        except:
            dic_result_df = self.get_data(df)
            dicRegression = {}
            for fund_index_code, fund_index_df in dic_result_df.items():
                fund_code = fund_index_code.split('_')[0]
                index_code = fund_index_code.split('_')[1]
                tempReturn = (fund_index_df - fund_index_df.shift(1)) / fund_index_df.shift(1)
                tempReturn.fillna(0, inplace=True)
                riskFree = 0.02 / 250
                fundReduceRf = tempReturn[fund_code] - riskFree
                bencReduceRf = tempReturn[index_code] - riskFree
                Y = fundReduceRf.values
                tmX1 = bencReduceRf.values
                tmX2 = np.power(tmX1, 2)
                TMResult = self.regression(tmX1, tmX2, Y)

                dicRegression[fund_code] = {}
                dicRegression[fund_code]['R方'] = round(TMResult.rsquared, 2)
                dicRegression[fund_code]['择股指标(年化alpha)'] = str(round(TMResult.params[0] * 252 * 100, 2)) + '%'
                dicRegression[fund_code]['择时指标(beta)'] = round(TMResult.params[2], 2)
                select_df = pd.DataFrame(dicRegression).T
                select_df.to_excel("择时选股能力.xlsx")

        target_bench_code = df.loc[self.target_code]['跟踪指数代码']
        same_total_df = df[df['跟踪指数代码'] == target_bench_code]
        estdate_str = (datetime.strptime(self.last_date_str,"%Y-%m-%d")-timedelta(days=365)).strftime("%Y-%m-%d")
        same_total_df = same_total_df[same_total_df['基金成立日']<=estdate_str]
        same_code_list = [fund_code for fund_code in same_total_df.index.tolist() if
                          df.loc[fund_code]['证券简称'].find('C') == -1]
        if len(same_code_list) >= 5:
            same_df = select_df.loc[same_code_list]
            self.logger.info('选取跟踪同样指数即%s'%(target_bench_code,))
        else:
            same_df = select_df
            self.logger.info('选取所有指数增强基金')

        alpha_sort_df = same_df.sort_values(by='择股指标(年化alpha)')
        esta_alpha_percent = (alpha_sort_df.index.tolist().index(self.target_code) + 1) / alpha_sort_df.shape[0]
        esta_alpha_percent_str = str(np.round(esta_alpha_percent * 100, 2)) + '%'

        beta_sort_df = same_df.sort_values(by='择时指标(beta)')
        esta_beta_percent = (beta_sort_df.index.tolist().index(self.target_code) + 1) / beta_sort_df.shape[0]
        esta_beta_percent_str = str(np.round(esta_beta_percent * 100, 2)) + '%'

        R_sort_df = same_df.sort_values(by='R方')
        esta_R_percent = (R_sort_df.index.tolist().index(self.target_code) + 1) / R_sort_df.shape[0]
        esta_R_percent_str = str(np.round(esta_R_percent * 100, 2)) + '%'

        self.logger.info("对所有运作时间超1年的的增强型指数基金（A/C类基金只统计数据最长的一类），根据其跟踪指数，利用TM回归模型，对其选股择时能力解析")
        self.logger.info("当前基金TM回归后的年化alpha为%s,选股能力占同类指数增强基金%s分位数。" % (
            same_df.loc[self.target_code]['择股指标(年化alpha)'], esta_alpha_percent_str))

        self.logger.info(
            '择时指标(beta)回归系数为%s，择时能力占同类%s分位数' % (same_df.loc[self.target_code]['择时指标(beta)'], esta_beta_percent_str))
        self.logger.info('TM回归解释程度%s'%(same_df.loc[self.target_code]['R方']))

    def get_main(self):
        df = pd.read_excel("指数增强基金2020-08-14.xlsx", index_col=0)
        self.get_total_info(df)
        self.get_select_judge(df)


if __name__ == '__main__':
    JudgeFundIndexImproveDemo = JudgeFundIndexImprove()
    JudgeFundIndexImproveDemo.get_main()
