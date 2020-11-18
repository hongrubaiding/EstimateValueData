# -- coding: utf-8 --


import pandas as pd
import mylog as mylog
import numpy as np
from datetime import datetime,timedelta


class JudgeFundImproveBase:
    def __init__(self):
        self.logger = mylog.set_log()

    def get_manager_info(self, company, df, target_code_list, manager_name):
        total_manager_list = df['基金经理(现任)'].tolist()
        fund_num_list = []
        for num in range(len(total_manager_list)):
            if isinstance(total_manager_list[num], str) and total_manager_list[num].find(manager_name) != -1:
                fund_num_list.append(num)
        target_df = df.iloc[fund_num_list]
        if target_df.empty:
            self.logger.info('未查询到基金经理%s管理过的指数增强型产品，对其投资经验需保持谨慎！')
            return
        self.logger.info('基金经理%s,查询到其管理指数增强型产品%s只(含A/C类),反映了该基金经理有一定相关产品的投资经验' % (manager_name, target_df.shape[0]))
        last_se = target_df[target_df['基金成立日'] == target_df['基金成立日'].min()].iloc[0]
        self.logger.info("其中，%s(%s)，于%s成立，为该基金经理管理的最早的跟踪%s产品，最新规模为%s亿元；" % (
            last_se['证券简称'], last_se.name, last_se['基金成立日'], target_code_list[0], last_se['基金规模亿元']))

        annual_alpha = last_se['Alpha(年化)_d_52_hs300百分']
        annual_alpha_str = str(np.round(annual_alpha, 2)) + '%'
        same_style_alpha = last_se['Alpha(年化)同类平均_d_52_hs300百分']
        same_style_alpha_str = str(np.round(same_style_alpha, 2)) + '%'
        total_alpha = last_se['Alpha_w_52_hs300百分']
        total_alpha_str = str(np.round(total_alpha, 2)) + '%'
        if annual_alpha > same_style_alpha:
            self.logger.info("该基金自成立以来，近一年超额alpha收益（相对沪深300）为%s，年化alpha收益为%s，高于同类平均的年化alpha收益%s" % (
            total_alpha_str, annual_alpha_str, same_style_alpha_str))
        else:
            self.logger.info("该基金自成立以来，近一年超额alpha收益（相对沪深300）为%s,年化alpha收益为%s，低于同类平均的年化alpha收益%s" % (
            total_alpha_str, annual_alpha_str, same_style_alpha_str))

        rate_name_list = [data_name for data_name in last_se.index.tolist() if data_name.find('回报排名') != -1]
        rate_se = last_se.loc[rate_name_list]

    def get_target_label_fund(self, company_fund_df, target_code_list=[]):
        bench_code_list = company_fund_df['跟踪指数代码'].tolist()
        num_list = [bench_code_num for bench_code_num in range(len(bench_code_list)) if bench_code_list[bench_code_num] in target_code_list]
        target_df = company_fund_df.iloc[num_list]
        if target_df.empty:
            return

        min_fund_esta = target_df['基金成立日'].min()
        min_fund_esta_se = target_df[target_df['基金成立日'] == min_fund_esta].iloc[0]
        min_fund_esta_name = min_fund_esta_se['证券简称']
        self.logger.info("其中，%s(%s)，基金经理%s,于%s成立，为该管理人旗下成立最早的跟踪%s产品，最新规模为%s亿元；" % (
            min_fund_esta_se['证券简称'], min_fund_esta_se.name, min_fund_esta_se['基金经理(现任)'], min_fund_esta_se['基金成立日'],
            target_code_list[0], min_fund_esta_se['基金规模亿元']))

        annual_alpha = min_fund_esta_se['Alpha(年化)_d_52_hs300百分']
        annual_alpha_str = str(np.round(annual_alpha, 2)) + '%'
        same_style_alpha = min_fund_esta_se['Alpha(年化)同类平均_d_52_hs300百分']
        same_style_alpha_str = str(np.round(same_style_alpha, 2)) + '%'
        if annual_alpha > same_style_alpha:
            self.logger.info("该基金自成立以来，年化alpha收益为%s，高于同类平均的alpha收益%s" % (annual_alpha_str, same_style_alpha_str))
        else:
            self.logger.info("该基金自成立以来，年化alpha收益为%s，低于同类平均的alpha收益%s" % (annual_alpha_str, same_style_alpha_str))
        return

    def get_comyany_info(self, company, df, target_code_list=[]):
        total_company_dic = {company_name: temp_df for company_name, temp_df in df.groupby(by='基金管理人')}
        total_company_esta = {company_name: temp_df['基金成立日'].min().strftime('%Y-%m-%d') for company_name, temp_df in
                              total_company_dic.items()}
        total_company_esta_se = pd.Series(total_company_esta, name='基金成立日').sort_values(ascending=False)
        esta_percent = (total_company_esta_se.index.tolist().index(company) + 1) / len(total_company_esta_se)
        esta_percent_str = str(np.round(esta_percent * 100, 2)) + '%'

        num_dic = {company_name: temp_df.shape[0] for company_name, temp_df in total_company_dic.items()}
        company_df = pd.DataFrame()
        num_se = pd.Series(num_dic, ).sort_values()
        per_rate = (list(num_se.unique()).index(num_se[company]) + 1) / len(list(num_se.unique()))
        per_rate_str = str(np.round(per_rate * 100, 4)) + '%'
        self.logger.info('%s旗下现有指数增强型产品共%s只(含A类C类),占所有管理人所持数量的%s分位数。' % (company, num_se[company], per_rate_str))
        if per_rate >= 0.7:
            self.logger.info("占比靠前，反映管理人在发行指数增强型基金上的优秀运作能力。")
        elif 0.7 >= per_rate > 0.4:
            self.logger.info("占比中等，管理人发行指数增强型基金的数量一般。")
        elif per_rate <= 0.4:
            self.logger.info('占比下游，管理人对指数增强型基金发行数量较少。')
        company_fund_df = df[df['基金管理人'] == company]
        min_fund_esta = company_fund_df['基金成立日'].min()
        min_fund_esta_se = company_fund_df[company_fund_df['基金成立日'] == min_fund_esta].iloc[0]
        min_fund_esta_name = min_fund_esta_se['证券简称']
        self.logger.info("其中，%s(%s)，基金经理%s,于%s成立，为该管理人旗下成立最早的增强型产品；" % (
            min_fund_esta_se['证券简称'], min_fund_esta_se.name, min_fund_esta_se['基金经理(现任)'], min_fund_esta_se['基金成立日']))
        self.logger.info("按各管理人发行最早指数增强型基金的时间看，该产品发行时间占各管理人同类型的%s分位数" % esta_percent_str)
        if esta_percent >= 0.6:
            self.logger.info("发行时间早与多数管理人，一定程度上反应了管理人更丰富的投资管理经验。")
        elif 0.3 <= esta_percent < 0.6:
            self.logger.info("发行时间排名中等，管理人整体投资管理经验中等水平")
        else:
            self.logger.info("发行时间较晚，需谨慎对待管理人可能对指数增强型基金投资管理经验较短的问题")

        if target_code_list:
            dic_target = {}
            for company_name, temp_df in df.groupby(by='基金管理人'):
                dic_target[company_name] = dic_target.get(company_name, 0)
                for bench_code in temp_df['跟踪指数代码'].tolist():
                    if bench_code in target_code_list:
                        dic_target[company_name] = dic_target[company_name]+1

            label_se = pd.Series(dic_target, name='产品数量')
            self.logger.info("跟踪%s指数的管理人共%s家" % (target_code_list[0], len(label_se[label_se > 0])))
            self.get_target_label_fund(company_fund_df, target_code_list)
        return

    def get_main(self, company='万家基金管理有限公司', manager_name='乔亮'):
        df = pd.read_excel('指数增强评价指标.xlsx', index_col=0)
        self.get_comyany_info(company, df, target_code_list=['000852.SH',])
        self.get_manager_info(company, df, target_code_list=['000852.SH',], manager_name=manager_name)

if __name__=='__main__':
    JudgeFundImproveBaseDemo = JudgeFundImproveBase()
    JudgeFundImproveBaseDemo.get_main()