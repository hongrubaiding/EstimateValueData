# -- coding: utf-8 --

'''
获取指数估值与ppt图表数据
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
from MonthReportData.GetTableData import GetTableData

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['axes.unicode_minus'] = False


class GetIndexEst:
    def __init__(self):
        self.start_date = (datetime.today() - timedelta(days=365 * 10)).strftime("%Y-%m-%d")
        # self.end_date = datetime.today().strftime("%Y-%m-%d")
        self.end_date = '2020-08-31'
        self.GetDataTotalMainDemo = GetDataTotalMain(data_resource='wind')
        self.file_path = os.getcwd() + '\\GetDataResult\\估值\\'
        self.file_month_path = os.getcwd() + '\\GetDataResult\\月度表现\\'

    def get_plot_figure(self, dic_df):
        for code, df in dic_df.items():
            temp_fig = plt.figure(figsize=(16, 9))
            temp_ax = temp_fig.add_subplot(111)
            df.plot(ax=temp_ax)
            temp_ax.grid()
            # temp_ax.set_title(u'%sPE走势' % code)
            plt.savefig(self.file_path + '%s估值走势图.png' % code)
            # plt.show()

    def get_regression(self, index_code_list, bench_code_list, total_return_df):

        for index_code in index_code_list:
            list_r2, list_beta, list_tr, list_const = [], [], [], []
            Y = total_return_df[index_code].values
            for bench_code in bench_code_list:
                x = total_return_df[bench_code].values
                x = x.reshape(len(x), 1)
                c = np.ones((len(x), 1))
                X = np.hstack((c, x))
                res = (sm.OLS(Y, X)).fit()
                list_r2.append(res.rsquared)
                list_beta.append(res.params[1])
                list_const.append(res.params[0])

            res_indus = pd.DataFrame([])
            res_indus['指数代码'] = bench_code_list

            res_indus['拟合R方'] = list_r2

            res_indus['beta'] = list_beta
            res_indus['alpha'] = list_const
            res_indus = res_indus.sort_values('拟合R方', ascending=False)
            res_indus.to_excel(self.file_path + '%s风格指数回归结果.xlsx' % index_code, index=False)

            maxR2Code = res_indus['指数代码'].tolist()[0]
            x = total_return_df[maxR2Code].values
            maxR2Alpha = res_indus['alpha'].tolist()[0]
            maxR2Beta = res_indus['beta'].tolist()[0]

            plt.style.use('ggplot')
            plt.figure(figsize=(16, 9))
            plt.scatter(x, Y, s=30, color='blue', label='样本实例')
            plt.plot(x, maxR2Alpha + maxR2Beta * x, linewidth=3, color='red', label='回归线')
            plt.ylabel('宽基指数超额收益')
            plt.xlabel('风格指数超额收益')
            # plt.title('%s拟合效果最好的风格指数：'%index_code +maxR2Code, fontsize=13,
            #           bbox={'facecolor': '0.8', 'pad': 5})
            plt.grid(True)
            plt.legend(loc='upper left')  # 添加图例
            plt.savefig(self.file_path + '%s拟合风格指数效果图.png' % index_code)
            # plt.show()

            plt.style.use('ggplot')
            fig = plt.figure(figsize=(16, 9))
            ax = fig.add_subplot(111)
            indeustryAccDf = (1 + total_return_df[[index_code, maxR2Code]]).cumprod()
            indeustryAccDf['指数收益比'] = indeustryAccDf[index_code] / indeustryAccDf[maxR2Code]
            indeustryAccDf.plot(ax=ax)
            ax.set_ylabel('累计收益率')
            ax.set_xlabel('时间')
            # ax.set_title('%s拟合效果最好的风格指数：'%index_code + maxR2Code, fontsize=13,
            #              bbox={'facecolor': '0.8', 'pad': 5})
            ax.grid(True)
            ax.legend(loc='down right')  # 添加图例
            plt.savefig(self.file_path + '%s拟合风格指数累计走势对比图.png' % index_code)

    def get_index_regress(self, index_code_list):
        bench_code_list = ['399314.SZ', '399315.SZ', '399316.SZ']
        df_list = []
        for code in bench_code_list:
            temp_df = self.GetDataTotalMainDemo.get_hq_data(code=code, start_date=self.start_date,
                                                            end_date=self.end_date, code_style='index')
            temp_df.rename(columns={'close_price': code}, inplace=True)
            df_list.append(temp_df)
        bench_df = pd.concat(df_list, axis=1, sort=True)

        df_list2 = []
        for code in index_code_list:
            temp_df = self.GetDataTotalMainDemo.get_hq_data(code=code, start_date=self.start_date,
                                                            end_date=self.end_date, code_style='index')
            temp_df.rename(columns={'close_price': code}, inplace=True)
            df_list2.append(temp_df)
        index_df = pd.concat(df_list2, axis=1, sort=True)

        total_df = pd.concat([index_df, bench_df], axis=1, sort=True)
        total_return_df = total_df / total_df.shift(1) - 1
        total_return_df.dropna(inplace=True)
        total_return_df.corr().to_excel(self.file_path + '相关系数.xlsx')

        self.get_regression(index_code_list, bench_code_list, total_return_df)

    def get_index_consit(self, index_code='000913.SH', weight=1):
        temp_df = self.GetDataTotalMainDemo.get_index_constituent(indexCode=index_code)
        wss_data = w.wss(codes=temp_df['stock_code'].tolist(),
                         fields=["industry_sw", "mkt_cap_ard", "roe_ttm", "yoyprofit", "dividendyield"],
                         options="industryType=1;unit=1;tradeDate=20200823;rptDate=20191231;rptYear=2019")
        code_ind_df = pd.DataFrame(wss_data.Data, index=wss_data.Fields, columns=wss_data.Codes).T
        name_dic = {"industry_sw".upper(): "申万一级行业", "mkt_cap_ard".upper(): "总市值",
                    "dividendyield".upper(): "股息率（2019年）",
                    "yoyprofit".upper(): "净利润同比增长率", "roe_ttm".upper(): "ROE"}
        code_ind_df.rename(columns=name_dic, inplace=True)
        try:
            use_df = temp_df[['stock_code', 'stock_weight', 'stock_name']].set_index('stock_code')
        except:
            a = 0

        stock_result_df = pd.concat([use_df, code_ind_df], sort=True, axis=1)
        df = pd.concat([use_df, code_ind_df], axis=1, sort=True)
        dic_ind_weight = {}
        for ind, stock_df in df.groupby('申万一级行业'):
            dic_ind_weight[ind] = stock_df['stock_weight'].sum() * weight / 100
        return dic_ind_weight, stock_result_df

    def calc_stock_weight(self, dic_stock_weight, index_se):
        for index_code, temp_df in dic_stock_weight.items():
            temp_df['port_stock_weight'] = temp_df['stock_weight'] * index_se[index_code]
        total_stock_df = pd.concat(list(dic_stock_weight.values()), axis=0, sort=True)
        total_stock_df['stock_code_label'] = total_stock_df.index.tolist()
        df_list = []
        for code, temp_stock_df in total_stock_df.groupby(by='stock_code_label'):
            if temp_stock_df.shape[0] > 1:
                target_df = temp_stock_df.iloc[0]
                target_df['port_stock_weight'] = temp_stock_df['port_stock_weight'].sum()
                target_df = pd.DataFrame(target_df).T
                df_list.append(target_df)
            else:
                df_list.append(temp_stock_df)
        total_stock_result = pd.concat(df_list, axis=0, sort=True).sort_values(by='port_stock_weight', ascending=False)
        name_dic = {'port_stock_weight': '权重', 'stock_name': '简称'}
        total_stock_result.rename(columns=name_dic).to_excel("股票持仓数据.xlsx")

    def get_port_weight(self, index_code_list=[], weight_list=[]):
        temp_se = pd.Series(weight_list, index=index_code_list)
        port_df_list = []
        dic_stock_weight = {}
        for index_code in index_code_list:
            dic_ind_weight, stock_weight_df = self.get_index_consit(index_code, weight=temp_se[index_code])
            dic_stock_weight[index_code] = stock_weight_df
            ind_weight_se = pd.Series(dic_ind_weight, name=index_code)
            port_df_list.append(ind_weight_se)

        self.calc_stock_weight(dic_stock_weight, temp_se)
        total_ind = pd.concat(port_df_list, axis=1, sort=True).sum(axis=1)
        total_ind.name = '组合行业暴露'

        bench_code_list = ['000300.SH', '000905.SH']
        bench_code_df_list = []
        for bench_code in bench_code_list:
            dic_bench_weight, _ = self.get_index_consit(bench_code)
            bench_weight_se = pd.Series(dic_bench_weight, name=bench_code)
            bench_code_df_list.append(bench_weight_se)
        bench_code_df = pd.concat(bench_code_df_list, axis=1, sort=True).rename(
            columns={'000300.SH': '沪深300', '000905.SH': "中证500"})
        total_df = pd.concat([total_ind, bench_code_df], axis=1, sort=True).fillna(0)
        total_df['相对沪深300'] = total_df['组合行业暴露'] - total_df['沪深300']
        total_df['相对中证500'] = total_df['组合行业暴露'] - total_df['中证500']
        total_df.to_excel("主题OTC组合暴露.xlsx")

    def get_init_param(self):
        code_list1 = ['399006.SZ', '399005.SZ', '000852.SH', '399001.SZ', '000905.SH', '000300.SH', '000001.SH',
                      '000016.SH']  # 宽基
        code_list2 = ['000935.SH', '000933.SH', '000932.SH', '000936.CSI', '000934.SH', '000931.CSI', '000930.CSI',
                      '000929.CSI', '000937.CSI', '000928.SH']  # 行业
        code_list3 = ['990001.CSI', '980017.CNI', '399803.SZ', '399973.SZ', '399441.SZ', '931066.CSI', '931087.CSI',
                      '000941.CSI', 'H30318.CSI', '931079.CSI', '931071.CSI', '399997.SZ', '399976.SZ', '399362.SZ',
                      'H30533.CSI', '399812.SZ', '399974.SZ', '000860.CSI', '000861.CSI', '000859.CSI',
                      '000015.SH']  # 主题
        code_list4 = ['399673.SZ', '399293.SZ', '399296.SZ', '399295.SZ', '930758.CSI', '399983.SZ', '000984.SH',
                      '000971.SH', '000982.SH', '399990.SZ', '399702.SZ', '000050.SH', '931052.CSI', '930838.CSI',
                      'H30269.CSI', '000925.CSI']  # 策略
        code_list5 = ['399377.SZ', '399348.SZ', '399919.SZ', '000029.SH']  # 风格
        dic_index = {}
        dic_index['宽基'] = code_list1
        dic_index['行业'] = code_list2
        dic_index['主题'] = code_list3
        dic_index['策略'] = code_list4
        dic_index['风格'] = code_list5
        return dic_index

    def get_main(self, ):
        dic_index_param = self.get_init_param()
        for index_type, index_code_list in dic_index_param.items():
            GetTableDataDemo = GetTableData()
            total_df = GetTableDataDemo.get_data(code_list=index_code_list, index_type=index_type)

            dic_df = {}
            dic_PE = {}
            for code in index_code_list:
                df = self.GetDataTotalMainDemo.get_hq_data(code=code, start_date=self.start_date,
                                                           end_date=self.end_date, code_style='index_daily',
                                                           dic_param={'fields': 'pe_ttm', 'filed_name': 'PE值'})
                df.rename(columns={'update_time': '时间', 'factor_value': "PE_TTM"}, inplace=True)
                last_value = df['PE_TTM'][-1]
                percent_num = (df['PE_TTM'].sort_values().tolist().index(last_value) + 1) / df.shape[0]
                print('%s当前估值分位数%s' % (code, round(percent_num, 4)))
                df.to_excel(self.file_path + '估值%s.xlsx' % code[:6])
                dic_df[total_df.loc[code]['证券简称']] = df
                dic_PE[code] = {'PE': last_value, 'PE分位数': percent_num}
            pe_df = pd.DataFrame(dic_PE).T
            total_last_df = pd.concat([total_df, pe_df], axis=1, sort=True)

            name_list = ['证券代码','证券简称', '近1月(%)', '近3月(%)', '近6月(%)', '近1年(%)', '近3年(%)', '今年以来(%)', '近一年最大回撤(%)', 'Sharp比率',
                         '年化波动(%)', '年化收益(%)', '月度成交额变化(%)', '月度换手率变化(%)',  'PE', 'PE分位数']
            total_last_df.to_excel(self.file_month_path + '%s指数月度表现.xlsx' % index_type, index=False)

            self.get_plot_figure(dic_df)



if __name__ == '__main__':
    GetIndexEstDemo = GetIndexEst()
    GetIndexEstDemo.get_main()
    # GetIndexEstDemo.get_index_consit()
    # GetIndexEstDemo.get_index_regress(code_list1)
    # ind_code_list = ['000913.SH', '000932.SH', '000988.CSI', '399986.SZ', '399995.SZ', '931008.CSI', '931009.CSI',
    #                  '000806.CSI']  #行业OTC
    # ind_weight_list = [0.038, 0.3, 0.214, 0.114, 0.06, 0.063, 0.135, 0.075]

    # zt_code_list = ['399814.SZ','930653.CSI','930743.CSI','930875.CSI','930914.CSI','h11136.CSI']   #主题OTC
    # zt_weight_list = [0.3089,0.0424,0.0605,0.2298,0.3089,0.0495]
    # GetIndexEstDemo.get_port_weight(index_code_list=ind_code_list, weight_list=ind_weight_list)
