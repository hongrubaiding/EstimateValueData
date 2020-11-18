# -- coding: utf-8 --


import pandas as pd
import mylog as mylog
import numpy as np
from datetime import datetime,timedelta
from GetAndSaveWindData.GetDataFromWindNNotMysql import GetDataFromWindNotMysql
import matplotlib.pyplot as plt
import matplotlib

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['font.family'] = 'sans-serif'
matplotlib.rcParams['axes.unicode_minus'] = False

class JudgeAndGetFund:
    def __init__(self):
        self.GetDataFromWindNotMysqlDemo = GetDataFromWindNotMysql(data_resource='wind')

    def get_init_param(self,fund_name='易方达行业领先'):
        dic_param={}
        base_df= pd.read_excel("参数_%s.xlsx"%fund_name,sheet_name='基础信息')
        bench_df= pd.read_excel("参数_%s.xlsx"%fund_name,sheet_name='业绩基准')
        self.start_date = base_df.iloc[0]['任职日期']
        self.end_date = base_df.iloc[0]['离任日期']
        self.fund_code  = base_df.iloc[0]['基金代码']
        return dic_param

    def get_fe_change(self,dic_param):
        df = self.GetDataFromWindNotMysqlDemo.get_fund_filed(start_date=self.start_date,end_date=self.end_date,fund_code=self.fund_code)
        df['基金份额变化率'] = df['基金份额_万份']/df['基金份额_万份'].shift(1)-1
        df['基金规模变化率'] = df['基金规模']/df['基金规模'].shift()-1
        fig_fe = plt.figure(figsize=(16,9))
        ax_fe = fig_fe.add_subplot(111)
        df['基金份额_万份'].plot.bar(ax=ax_fe)
        ax_fe.set_title('基金份额_万份')
        plt.savefig('基金份额概况.png')

        fig_size = plt.figure(figsize=(16,9))
        ax_size = fig_size.add_subplot(111)
        wid = 0.5
        df['基金规模'].plot(kind='bar',ax=ax_size,color='r',width=wid)
        ax_size.set_xticklabels(df.index,rotation=90)
        ax_size.set_title('基金规模')
        plt.savefig('基金规模概况.png')

        fig_stock_rate = plt.figure(figsize=(16, 9))
        ax_stock_rate = fig_stock_rate.add_subplot(111)
        (df['股票市值占基金资产净值比']/100).plot(kind='bar',ax=ax_stock_rate,color='b',)
        ax_stock_rate.set_title('股票市值占基金资产净值比')
        plt.savefig('股票占比情况.png')
        # plt.show()
        df.to_excel("%s份额规模概况.xlsx"%self.fund_code)

    def sum_plot(self,df):
        '''
        十大重仓股占比与绘图
        '''
        dic_sum={}
        dic_indus_fund_sum={}
        dic_indus_stock_sum={}
        for datestr,temp_df in df.groupby(by='披露日期'):
            dic_indus_fund_sum[datestr] = {}
            dic_indus_stock_sum[datestr] = {}
            for indus,detail_df in temp_df.groupby('所属行业'):
                dic_indus_fund_sum[datestr][indus] = detail_df['市值占基金资产净值比'].sum()
                dic_indus_stock_sum[datestr][indus]= detail_df['市值占股票投资市值比'].sum()
            dic_sum[datestr]={'十大重仓股市值占基金净值比':temp_df['市值占基金资产净值比'].sum(),'十大重仓股市值占股票投资市值比':temp_df['市值占股票投资市值比'].sum()}
        value_fund_df = pd.DataFrame(dic_indus_fund_sum).T / 100
        value_fund_df.fillna(0, inplace=True)

        value_stock_df = pd.DataFrame(dic_indus_stock_sum).T / 100
        value_stock_df.fillna(0, inplace=True)

        sum_df = pd.DataFrame(dic_sum).T
        fig = plt.figure(figsize=(16,9))
        ax = fig.add_subplot(111)
        sum_df.plot.bar(ax=ax)

        color = ['#36648B', '#458B00', '#7A378B', '#8B0A50', '#8FBC8F', '#B8860B', '#FFF68F', '#FFF5EE', '#FFF0F5',
                 '#FFEFDB',
                 '#F4A460', '#A0522D', '#FFE4E1', '#BC8F8F', '#A52A2A', '#800000', '#F5F5F5', '#DCDCDC', '#808080',
                 '#000000',
                 '#FFA500', '#F5DEB3', '#DAA520', '#BDB76B', '#556B2F', '#006400', '#98FB98', '#7FFFAA', '#20B2AA',
                 '#F0FFFF',
                 '#191970', '#BA55D3', '#DDA0DD', '#4B0082', '#8FBC8F', '#B8860B', '#FFF68F', '#FFF5EE', '#FFF0F5',
                 '#FFEFDB',
                 '#36648B', '#458B00', '#7A378B', '#8B0A50', '#8FBC8F', '#B8860B', '#FFF68F', '#FFF5EE', '#FFF0F5',
                 '#FFEFDB']
        fig2 = plt.figure(figsize=(16, 9))
        ax2 = fig2.add_subplot(111)
        datestrList = value_fund_df.index.tolist()
        labels = value_fund_df.columns.tolist()
        for i in range(value_fund_df.shape[1]):
            ax2.bar(datestrList, value_fund_df.ix[:, i], color=color[i],
                    bottom=value_fund_df.ix[:, :i].sum(axis=1),)

        box = ax2.get_position()
        ax2.set_position([box.x0, box.y0, box.width * 1.02, box.height])
        ax2.legend(labels=labels, bbox_to_anchor=(1, 0.8), ncol=1)
        ax2.set_title("重仓行业市值占基金资产净值比")
        for tick in ax2.get_xticklabels():
            tick.set_rotation(90)
        plt.savefig('重仓行业市值占基金资产净值比.png')

        fig3 = plt.figure(figsize=(16, 9))
        ax3 = fig3.add_subplot(111)
        datestrList2 = value_stock_df.index.tolist()
        labels2 = value_stock_df.columns.tolist()
        for i in range(value_stock_df.shape[1]):
            ax3.bar(datestrList2, value_stock_df.ix[:, i], color=color[i],
                    bottom=value_stock_df.ix[:, :i].sum(axis=1), )
        box2 = ax3.get_position()
        ax3.set_position([box2.x0, box2.y0, box2.width * 1.02, box2.height])
        ax3.legend(labels=labels2, bbox_to_anchor=(1, 0.8), ncol=1)
        ax3.set_title("重仓行业市值占股票投资净值比")
        for tick in ax3.get_xticklabels():
            tick.set_rotation(90)
        plt.savefig('重仓行业市值占股票投资净值比.png')
        plt.show()

    def get_stock_diff(self,df):
        temp_df = df.copy()
        temp_df = temp_df.set_index(keys=['披露日期','重仓排名'])
        total_date_list = list(df['披露日期'].unique())
        result_df = pd.DataFrame()
        change_name_list=['市值占基金资产净值比','市值占股票投资市值比','持股市值','股票代码']
        for date_num in range(len(total_date_list)):
            if date_num==0:
                target_df = temp_df.loc[total_date_list[date_num]][change_name_list].set_index('股票代码')
            else:
                current_df = temp_df.loc[total_date_list[date_num]][change_name_list].set_index('股票代码')
                pre_df = temp_df.loc[total_date_list[date_num-1]][change_name_list].set_index('股票代码')
                a=0


            a=0

    def get_stock_detail(self,dic_param):
        try:
            df = pd.read_excel("%s重仓股概况.xlsx"%self.fund_code,index_col=0,converters={'股票代码':str,'重仓排名':int})
        except:
            df = self.GetDataFromWindNotMysqlDemo.get_fund_stock_filed(start_date=self.start_date,end_date=self.end_date,fund_code=self.fund_code)
            df.to_excel('%s重仓股概况.xlsx'%self.fund_code)
        # df = self.GetDataFromWindNotMysqlDemo.get_fund_stock_filed(start_date=self.start_date, end_date=self.end_date,
        #                                                            fund_code=self.fund_code)
        # df.to_excel('%s重仓股概况.xlsx' % self.fund_code)
        df['披露日期']=[datetime.strftime(dateStr,"%Y-%m-%d") for dateStr in df.index.tolist()]
        df.dropna(inplace=True)
        # self.sum_plot(df)
        self.get_stock_diff(df)



    def get_main(self):
        dic_param = self.get_init_param()
        # self.get_fe_change(dic_param)
        self.get_stock_detail(dic_param)


if __name__=='__main__':
    JudgeAndGetFundDemo = JudgeAndGetFund()
    JudgeAndGetFundDemo.get_main()