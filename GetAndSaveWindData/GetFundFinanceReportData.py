# -- coding: utf-8 --

'''
    将wind/ifind的数据导入到本地数据库,并从数据库返回结果
'''

import pandas as pd
from GetAndSaveWindData.GetDataToMysql import GetDataToMysql
import mylog as mylog
import numpy as np
from datetime import datetime



class GetFundFinanceReportData:
    def __init__(self):
        self.logger = mylog.set_log()
        self.GetDataToMysqlDemo = GetDataToMysql()

    def get_fund_stock_info(self, third_conn, engine, total_date_list, fund_code='100053.OF'):
        rpt_date_str_list = []
        for rpt_date in total_date_list:
            if rpt_date[-5:]=='06-30':
                name_str = rpt_date[:4]+'年中报'
            else:
                name_str = rpt_date[:4]+'年年报'
            rpt_date_str_list.append(name_str)
        sql_str = "select * from fund_contain_stock_detail where rpt_date in %s and fund_code='%s'" % (
            str(tuple(rpt_date_str_list)), fund_code)
        result_df = pd.read_sql(sql=sql_str, con=engine)
        have_rpt_str_list = result_df['rpt_date'].tolist()
        lack_rpt_list = [rpt_date for rpt_date in rpt_date_str_list if rpt_date not in have_rpt_str_list]
        name_mysql_dic = {'sec_name': 'fund_name', 'marketvalueofstockholdings': 'market_value_of_stockholdings',
                          'proportiontototalstockinvestments': 'pro_total_stock_inve',
                          'proportiontonetvalue': 'pro_net_value',
                          'proportiontoshareholdtocirculation': 'pro_sharehold_cir'}
        if lack_rpt_list:
            temp_df_list = []
            for lack_rpt in lack_rpt_list:
                lack_date = total_date_list[rpt_date_str_list.index(lack_rpt)]
                rptdate = ''.join(lack_date.split('-'))
                options = "rptdate=%s;windcode=%s" % (rptdate, fund_code)
                wset_data = third_conn.wset(tablename="allfundhelddetail", options=options)
                if wset_data.ErrorCode != 0:
                    self.logger.error('wind获取基金持股明细数据错误，错误代码%s，请检查！' % wset_data.ErrorCode)
                    return pd.DataFrame()
                temp_rpt_df = pd.DataFrame(wset_data.Data, index=wset_data.Fields, columns=wset_data.Codes).T
                if temp_rpt_df.empty:
                    continue
                temp_rpt_df['fund_code'] = fund_code
                temp_rpt_df['record_time'] = datetime.today().strftime("%Y-%m-%d")
                temp_rpt_df.rename(columns=name_mysql_dic, inplace=True)
                self.GetDataToMysqlDemo.GetMain(temp_rpt_df, 'fund_contain_stock_detail')
                self.logger.info("存储%s,报告期%s持股数据成功！" % (fund_code, lack_rpt))
                temp_df_list.append(temp_rpt_df)
            if temp_df_list:
                temp_df = pd.concat(temp_df_list, axis=0, sort=True)
                result_df = pd.concat([result_df, temp_df], axis=0, sort=True)
        return result_df

    def get_main(self):
        pass
