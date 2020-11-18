# -- coding: utf-8 --

'''
    获取wind数据，不保存mysql,部分非常规的书
'''

# from WindPy import w
import pandas as pd
from iFinDPy import *
import mylog as mylog
import WindPy as Wind


class GetDataFromWindNotMysql:
    def __init__(self, data_resource='ifind'):
        self.logger = mylog.set_log()
        self.dic_init = {}
        self.dic_init['data_resource'] = data_resource
        self.dic_init['data_init_flag'] = self.log_init(data_resource)

    def log_init(self, data_resource='ifind'):
        '''
        登录客户端初始化
        :param data_resource:
        :return:
        '''
        flag = True
        if data_resource == 'ifind':
            log_state = THS_iFinDLogin('zszq5072', '754628')
            if log_state == 0:
                self.logger.info("同花顺账号登录成功！")
            else:
                self.logger.error("同花顺账号登录异常，请检查！")
                flag = False
        elif data_resource == 'wind':
            try:
                Wind.w.start()
            except:
                self.logger.info("wind启动失败")
                flag = False
        return flag

    def get_fund_stock_filed(self, start_date, end_date, fund_code=''):
        fund_df = pd.DataFrame()
        fileds = ['prt_topstockname', 'prt_topstockcode', 'prt_topstockvalue', 'prt_heavilyheldstocktostock',
                  'prt_heavilyheldstocktonav']
        name_Dic = {'prt_topstockname'.upper(): '股票名称', 'prt_topstockcode'.upper(): '股票代码',
                    'prt_topstockvalue'.upper(): '持股市值', 'prt_heavilyheldstocktostock'.upper(): '市值占股票投资市值比',
                    'prt_heavilyheldstocktonav'.upper(): '市值占基金资产净值比'}
        df_list=[]
        for order_num in range(1,11):
            wsddata = Wind.w.wsd(codes=fund_code, fields=fileds, beginTime=start_date, endTime=end_date,
                                 options="order=%s;unit=1;Period=Q;Days=Alldays"%order_num)
            if wsddata.ErrorCode != 0:
                self.logger.error("获取重仓股数据有误，错误代码" + str(wsddata.ErrorCode))
                continue
            temp_fund_df = pd.DataFrame(wsddata.Data, index=wsddata.Fields, columns=wsddata.Times).T
            temp_fund_df['重仓排名']= order_num
            df_list.append(temp_fund_df)
        if df_list:
            fund_df = pd.concat(df_list,axis=0,sort=True)
        fund_df.rename(columns=name_Dic,inplace=True)
        fund_df['披露日期'] = fund_df.index.tolist()
        fund_df.dropna(inplace=True)

        indus_list = []
        for datestr,temp_df in fund_df.groupby(by='披露日期'):
            code_init=list(temp_df['股票代码'].tolist())
            code_list = []
            for code in code_init:
                if code[0]=='6':
                    codestr=code+'.SH'
                elif code[0] in ['0','3']:
                    codestr = code+'.SZ'
                code_list.append(codestr)
            # tradeDate = datestr[:4]+datestr[5:7]+datestr[8:10]
            tradeDate = datetime.strftime(datestr,"%Y%m%d")
            param_list = list(set(code_list))
            wssdata = Wind.w.wss(codes=param_list,fields=['industry_citic'],options='tradeDate=%s;industryType=1'%tradeDate)
            if wssdata.ErrorCode != 0:
                self.logger.error("获取股票所属行业数据有误，错误代码" + str(wssdata.ErrorCode))
                continue
            # temp_fund_df = pd.DataFrame(wssdata.Data, columns=wssdata.Codes, index=wssdata.Fields).T
            temp_se = pd.Series(wssdata.Data[0],index=wssdata.Codes,name='所属行业')
            indus_list = indus_list+[temp_se[code] for code in code_list]
        fund_df['所属行业'] = indus_list
        return fund_df

    def get_fund_filed(self, start_date, end_date, fund_code=''):
        '''
            基金季度数据，
            基金份额，基金规模，股票资产占基金净资产比例
        '''
        # total_date_list = w.tdays(start_date, end_date, "Days=Alldays;Period=Q")
        fund_df = pd.DataFrame()
        fileds = ['unit_fundshare_total', 'netasset_total', 'prt_stocktonav']
        name_Dic = {'unit_fundshare_total'.upper(): '基金份额_万份', 'netasset_total'.upper(): '基金规模',
                    'prt_stocktonav'.upper(): '股票市值占基金资产净值比'}
        wsddata = Wind.w.wsd(codes=fund_code, fields=fileds, beginTime=start_date, endTime=end_date,
                             options="unit=1;Period=Q;Days=Alldays")
        if wsddata.ErrorCode != 0:
            self.logger.error("获取全A股数据有误，错误代码" + str(wsddata.ErrorCode))
            return fund_df
        fund_df = pd.DataFrame(wsddata.Data, index=wsddata.Fields, columns=wsddata.Times).T
        fund_df.rename(columns=name_Dic, inplace=True)
        return fund_df
