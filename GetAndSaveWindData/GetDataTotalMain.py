# -- coding: utf-8 --

'''
    将wind/ifind的数据导入到本地数据库,并从数据库返回结果
'''

from WindPy import *
import pandas as pd
from GetAndSaveWindData.MysqlCon import MysqlCon
from iFinDPy import *
from GetAndSaveWindData.GetDataToMysql import GetDataToMysql
import mylog as mylog
from WindPy import w
import numpy as np
from GetAndSaveWindData.GetFundFinanceReportData import GetFundFinanceReportData


class GetDataTotalMain:
    def __init__(self, data_resource='ifind'):
        self.logger = mylog.set_log()
        self.dic_init = {}
        self.dic_init['data_resource'] = data_resource
        self.dic_init['data_init_flag'] = self.log_init(data_resource)
        mysql_con_demo = MysqlCon()
        self.engine = mysql_con_demo.getMysqlCon(flag='engine')
        self.conn = mysql_con_demo.getMysqlCon(flag='connect')
        self.GetDataToMysqlDemo = GetDataToMysql()

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
                w.start()
            except:
                self.logger.info("wind启动失败")
                flag = False
        return flag

    def get_index_constituent(self, indexCode='000300.SH', getDate='2020-08-21'):
        '''
        获取指数成分股
        :param indexCode:指数代码
        :param getDate:获取日期
        :return:df,指数代码、成分股代码、成分股名称，权重
        '''
        sqlStr = "select * from index_constituent where index_code='%s' and update_time='%s'" % (indexCode, getDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        nameDic = {'date': 'adjust_time', 'wind_code': 'stock_code', "sec_name": 'stock_name',
                   'i_weight': 'stock_weight', 'DATE': 'adjust_time', 'THSCODE': 'stock_code',
                   "SECURITY_NAME": 'stock_name',
                   'WEIGHT': 'stock_weight'}
        if resultDf.empty:
            if self.dic_init['data_resource'] == 'wind':
                wsetdata = w.wset("indexconstituent", "date=%s;windcode=%s" % (getDate, indexCode))
                if wsetdata.ErrorCode != 0:
                    self.logger.error("获取指数成分股数据有误，错误代码" + str(wsetdata.ErrorCode))
                    return pd.DataFrame()
                resultDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields).T
                if resultDf.empty:
                    wsetdata = w.wset("sectorconstituent", "date=%s;windcode=%s" % (getDate, indexCode))
                    if wsetdata.ErrorCode != 0:
                        self.logger.error("获取板块指数成分股数据有误，错误代码" + str(wsetdata.ErrorCode))
                        return pd.DataFrame()
                resultDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields).T
                if resultDf.empty:
                    self.logger.info("指定日期内，未找到有效成分股数据")
                    return pd.DataFrame()
                dateList = [datetampStr.strftime('%Y-%m-%d') for datetampStr in resultDf['date'].tolist()]
                resultDf['date'] = dateList
                if 'industry' in resultDf:
                    resultDf.drop(labels='industry', inplace=True, axis=1)
            elif self.dic_init['data_resource'] == 'ifind':
                param_name = '%s;%s' % (getDate, indexCode)
                fun_option = 'date:Y,thscode:Y,security_name:Y,weight:Y'
                ths_data = THS_DataPool(DataPoolname='index', paramname=param_name, FunOption=fun_option, outflag=False)
                if ths_data['errorcode'] != 0:
                    self.logger.error("同花顺获取指数成分股数据错误，请检查:%s" % ths_data['errmsg'])
                    return pd.DataFrame()
                resultDf = THS_Trans2DataFrame(ths_data)
            resultDf.rename(columns=nameDic, inplace=True)
            resultDf['update_time'] = getDate
            resultDf['index_code'] = indexCode
            self.GetDataToMysqlDemo.GetMain(resultDf, 'index_constituent')
        return resultDf

    def get_hq_data_to_Mysql(self, code, start_date='2019-04-01', end_date='2019-04-30', code_style='index',
                             dic_param={}):
        '''
        获取指定的行情数据，保存至本地数据库
        :param code:
        :param start_date:
        :param end_date:
        :param code_style:
        :return:
        '''
        if code_style == 'index_daily':
            self.get_index_daily_data(filed_name=dic_param['filed_name'], fields=dic_param['fields'],
                                      start_date=start_date, end_date=end_date,code_list=[code])
            return

        tempDf = pd.DataFrame()
        if code_style == 'index':
            code_style_name = 'index_code'
            table_str = 'index_value'
            if self.dic_init['data_resource'] == 'ifind':
                if code[-3:] == '.CS':
                    code = code[:-3] + '.CB'

                name_dic = {'open': "open_price", "high": "high_price", "low": "low_price", "close": "close_price",
                            "changeRatio": "pct_chg", "turnoverRatio": "turn", "change": "chg", 'time': "update_time",
                            "thscode": "index_code"}
                data_fileds = 'open,high,low,close,volume,changeRatio,turnoverRatio,change'
                data_fileds_params = 'Interval:D,CPS:6,baseDate:1900-01-01,Currency:YSHB,fill:Previous'
                ths_data = THS_HistoryQuotes(thscode=code, jsonIndicator=data_fileds, jsonparam=data_fileds_params,
                                             begintime=start_date, endtime=end_date, outflag=False)
                if ths_data['errorcode'] != 0:
                    self.logger.error("同花顺获取行情数据错误，请检查:%s" % ths_data['errmsg'])
                    return pd.DataFrame()
                tempDf = THS_Trans2DataFrame(ths_data)

            elif self.dic_init['data_resource'] == 'wind':
                name_dic = {"OPEN": "open_price", "HIGH": "high_price", "LOW": "low_price", "CLOSE": "close_price",
                            "VOLUME": "volume", "AMT": "amt", "CHG": "chg", "PCT_CHG": "pct_chg", "TURN": "turn", }
                data_fileds = ["open", "high", "low", "close", "volume", "amt", "chg", "pct_chg", "turn"]  # 要获取的数据字段
                data_fileds_params = "PriceAdj=F"
                wsetdata = w.wsd(codes=code, fields=data_fileds, beginTime=start_date, endTime=end_date,
                                 options=data_fileds_params)
                if wsetdata.ErrorCode != 0:
                    self.logger.error("获取行情数据有误，错误代码" + str(wsetdata.ErrorCode))
                    return pd.DataFrame()
                tempDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Times).T
                dateList = [dateStr.strftime("%Y-%m-%d") for dateStr in tempDf.index.tolist()]
                tempDf['update_time'] = dateList
                tempDf[code_style_name] = code
            tempDf.dropna(how='all', inplace=True)
            tempDf.rename(columns=name_dic, inplace=True)
            self.GetDataToMysqlDemo.GetMain(tempDf, table_str)
        elif code_style == 'fund':
            code_style_name = 'fund_code'
            table_str = 'fund_net_value'
            if self.dic_init['data_resource'] == 'ifind':
                name_dic = {'time': "update_time", "thscode": "fund_code", "ths_unit_nv_fund": "net_value",
                            "ths_accum_unit_nv_fund": "acc_net_value"}
                data_fileds = 'ths_unit_nv_fund;ths_accum_unit_nv_fund'
                data_fileds_params = 'Days:Tradedays,Fill:Previous,Interval:D'

                ths_data = THS_DateSerial(thscode=code, jsonIndicator=data_fileds, globalparam=data_fileds_params,
                                          jsonparam=';', begintime=start_date, endtime=end_date, outflag=False)
                if ths_data['errorcode'] != 0:
                    self.logger.error("同花顺获取基金净值数据错误，请检查:%s" % ths_data['errmsg'])
                    return pd.DataFrame()
                tempDf = THS_Trans2DataFrame(ths_data)
            elif self.dic_init['data_resource'] == 'wind':
                name_dic = {"NAV": "net_value", "NAV_ACC": "acc_net_value", "NAV_ADJ": "net_value_adj"}
                wsddata = w.wsd(code, "nav,NAV_acc,NAV_adj", start_date, end_date, "")
                if wsddata.ErrorCode != 0:
                    self.logger.error("获取基金净值数据有误，错误代码" + str(wsddata.ErrorCode))
                    return pd.DataFrame()
                tempDf = pd.DataFrame(wsddata.Data, index=wsddata.Fields, columns=wsddata.Times).T
                dateList = [dateStr.strftime("%Y-%m-%d") for dateStr in tempDf.index.tolist()]
                tempDf['update_time'] = dateList
                tempDf[code_style_name] = code
            tempDf.dropna(how='all', inplace=True)
            tempDf.rename(columns=name_dic, inplace=True)
            self.GetDataToMysqlDemo.GetMain(tempDf, table_str)
        elif code_style == 'etf_fund':
            code_style_name = 'fund_code'
            table_str = 'etf_hq_value'
            if self.dic_init['data_resource'] == 'ifind':
                name_dic = {'open': "open_price", "high": "high_price", "low": "low_price", "close": "close_price",
                            "changeRatio": "pct_chg", "turnoverRatio": "turn", "change": "chg", 'time': "update_time",
                            "thscode": "fund_code", "avgPrice": "vwap"}
                data_fileds = 'open,high,low,close,volume,changeRatio,turnoverRatio,change'
                data_fileds_params = 'Interval:D,CPS:6,baseDate:1900-01-01,Currency:YSHB,fill:Previous'
                ths_data = THS_HistoryQuotes(thscode=code, jsonIndicator=data_fileds, jsonparam=data_fileds_params,
                                             begintime=start_date, endtime=end_date, outflag=False)
                if ths_data['errorcode'] != 0:
                    self.logger.error("同花顺获取行情数据错误，请检查:%s" % ths_data['errmsg'])
                    return pd.DataFrame()
                tempDf = THS_Trans2DataFrame(ths_data)

            elif self.dic_init['data_resource'] == 'wind':
                name_dic = {"OPEN": "open_price", "HIGH": "high_price", "LOW": "low_price", "CLOSE": "close_price",
                            "VOLUME": "volume", "AMT": "amt", "CHG": "chg", "PCT_CHG": "pct_chg", "TURN": "turn",
                            'VWAP': 'vwap'}
                data_fileds = ["open", "high", "low", "close", "volume", "amt", "chg", "pct_chg", "turn",
                               "vwap"]  # 要获取的数据字段
                data_fileds_params = "PriceAdj=F"
                wsetdata = w.wsd(codes=code, fields=data_fileds, beginTime=start_date, endTime=end_date,
                                 options=data_fileds_params)
                if wsetdata.ErrorCode != 0:
                    self.logger.error("获取行情数据有误，错误代码" + str(wsetdata.ErrorCode))
                    return pd.DataFrame()
                tempDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Times).T
                tempDf[tempDf.isnull()] = np.nan
                tempDf.dropna(how='all', inplace=True)
                if tempDf.empty:
                    self.logger.info("wind获取%s etf基金数据为空，请检查" % code)
                    return
                dateList = [dateStr.strftime("%Y-%m-%d") for dateStr in tempDf.index.tolist()]
                tempDf['update_time'] = dateList
                tempDf[code_style_name] = code
                tempDf['record_time'] = datetime.today().strftime("%Y-%m-%d")
            tempDf.dropna(how='all', inplace=True)
            tempDf.rename(columns=name_dic, inplace=True)
            self.GetDataToMysqlDemo.GetMain(tempDf, table_str)

    def get_lackdata_to_MySql(self, code, startDate, endDate, code_style='index', dic_param={}):
        '''
        获取指定时间内的行情数据,保存至本地数据库
        :param code: 代码：指数（股票，基金待兼容）
        :param startDate: 开始日期
        :param endDate: 截止日期
        :return:
        '''
        self.logger.info("检查本地%s,%s-%s缺失行情数据..." % (code, startDate, endDate))
        if code_style == 'index':
            tableStr = 'index_value'
            codeName = 'index_code'
        elif code_style == 'fund':
            tableStr = 'fund_net_value'
            codeName = 'fund_code'
        elif code_style == 'stock':
            tableStr = 'stock_hq_value'
            codeName = 'stock_code'
        elif code_style == 'etf_fund':
            tableStr = 'etf_hq_value'
            codeName = 'fund_code'
        elif code_style == 'index_daily':
            tableStr = 'index_daily_data'
            codeName = 'index_code'
        sqlStr = "select max(update_time),min(update_time) from %s where %s='%s'" % (tableStr, codeName, code)
        cursor = self.conn.cursor()
        cursor.execute(sqlStr)
        dateStrTuple = cursor.fetchall()[0]
        maxDate = dateStrTuple[0]
        minDate = dateStrTuple[1]

        codetion1 = (not maxDate) or (endDate < minDate or startDate > maxDate)
        if codetion1:
            start_date = startDate
            end_date = endDate
        elif startDate <= minDate:
            if minDate <= endDate < maxDate:
                if startDate != minDate:
                    start_date = startDate
                    end_date = minDate
                else:
                    return
            elif endDate >= maxDate:
                if startDate != minDate:
                    self.get_hq_data_to_Mysql(code, start_date=startDate, end_date=minDate, code_style=code_style,
                                              dic_param=dic_param)
                if endDate != maxDate:
                    start_date = maxDate
                    end_date = endDate
                else:
                    return
        elif endDate > maxDate:
            start_date = maxDate
            end_date = endDate
        elif startDate >= minDate and endDate <= maxDate:
            return

        if (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d") <= start_date:
            self.logger.info("行情数据起止时间间隔不够，不用补全。startdate: %s;enddate: %s" % (start_date, end_date))
            return
        else:
            self.get_hq_data_to_Mysql(code, start_date=start_date, end_date=end_date, code_style=code_style,
                                      dic_param=dic_param)

    def get_date_from_MySql(self, code, start_date, end_date, code_style='index', name_list=['close_price'],dic_param={}):
        if not name_list:
            self.logger.error('传入获取指数的字段不合法，请检查！')

        if code_style == 'index':
            table_str = 'index_value'
            code_name = 'index_code'
        elif code_style == 'fund':
            table_str = 'fund_net_value'
            code_name = "fund_code"
        elif code_style == 'stock':
            table_str = 'stock_hq_value'
            code_name = 'stock_code'
        elif code_style == 'etf_fund':
            table_str = 'etf_hq_value'
            code_name = 'fund_code'
        elif code_style=='index_daily':
            table_str = 'index_daily_data'
            code_name = 'index_code'

        if code_style!='index_daily':
            sqlStr = "select %s,update_time from %s where %s='%s' and  update_time>='%s'" \
                     " and update_time<='%s'" % (','.join(name_list), table_str, code_name, code, start_date, end_date)
        else:
            sqlStr = "select factor_value,update_time from %s where %s='%s' and factor_get_str='%s' and  update_time>='%s'" \
                     " and update_time<='%s'" % (table_str, code_name, code, dic_param['fields'],start_date, end_date)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        resultDf = resultDf.drop_duplicates('update_time').sort_index()
        resultDf.set_index(keys='update_time', inplace=True, drop=True)
        return resultDf

    def get_hq_data(self, code, start_date, end_date, code_style='index', name_list=['close_price'],dic_param={}):
        '''
        获取行情数据主入口
        :param code:
        :param start_date:
        :param end_date:
        :param code_syle:
        :param name_list:
        :return:
        '''
        self.get_lackdata_to_MySql(code, start_date, end_date, code_style,dic_param=dic_param)
        resultDf = self.get_date_from_MySql(code, start_date, end_date, code_style, name_list,dic_param=dic_param)
        return resultDf

    def get_tradeday(self, start_date, end_date, period='M'):
        '''
        获取指定周期交易日
        :param start_date:
        :param end_date:
        :param period: ''日，W周，M月，Q季，S半年，Y年
        :return:
        '''
        if self.dic_init['data_resource'] == 'wind':
            data = w.tdays(beginTime=start_date, endTime=end_date, options="Period=%s" % period)
            if data.ErrorCode != 0:
                self.logger.error('wind获取交易日期错误，请检查！')
                return
            tradeDayList = data.Data[0]
            tradeDayList = [tradeDay.strftime('%Y-%m-%d') for tradeDay in tradeDayList]
        elif self.dic_init['data_resource'] == 'ifind':
            ths_data = THS_DateQuery('SSE', 'dateType:0,period:%s,dateFormat:0' % period, start_date, end_date)
            if ths_data['errorcode'] != 0:
                self.logger.error("同花顺获取交易日期数据错误，请检查:%s" % ths_data['errmsg'])
                return
            tradeDayList = ths_data['tables']['time']
        return tradeDayList

    def facort_wind_ifind_to_mysql(self):
        '''
        wind与ifind原始因子简称，存入到本地数据库时的映射
        :return:
        '''
        name_dic = {}
        name_dic['mkt_freeshares'] = "MKT_FREESHARES"
        name_dic['pe_ttm'] = "PE_TTM"
        name_dic['ps_ttm'] = "PS_TTM"
        name_dic['pct_chg'] = "PCT_CHG"
        name_dic['rating_avg'] = "RATING_AVG"

        name_dic['ths_pe_ttm_stock'] = "PE_TTM"
        name_dic['ths_ps_ttm_stock'] = "PS_TTM"
        name_dic['ths_chg_ratio_m_stock'] = "PCT_CHG"
        name_dic['ths_compre_rating_value_stock'] = "RATING_AVG"
        name_dic['ths_free_float_mv_stock'] = "MKT_FREESHARES"
        name_dic['ths_pb_mrq_stock'] = 'PB_MRQ'
        name_dic['ths_beta_24m_stock'] = "BETA_24M"
        return name_dic

    def get_stock_month_to_MySql(self, code_list, factor_list, start_date='2011-11-01', end_date='2019-12-31'):
        '''
        获取全A股月度因子数据,保存至本地数据库
        注：（1）因子参数需谨慎检查（2）该方法不适宜回测调用，应一次调用存入数据库后，使用get_factor_value方法
        :return:
        '''
        self.logger.info("获取股票月度因子数据，请确保因子在wind与ifind的参数名称")
        total_trade_list = self.get_tradeday(start_date, end_date, period='M')
        name_dic = self.facort_wind_ifind_to_mysql()
        for trade_date in total_trade_list:
            if self.dic_init['data_resource'] == 'wind':
                factor_list = ["mkt_freeshares", "pe_ttm", "ps_ttm", "pct_chg", "rating_avg"]
                optionstr = "tradeDate=%s;cycle=M" % (trade_date[:4] + trade_date[5:7] + trade_date[8:])
                wssdata = w.wss(codes=code_list, fields=factor_list, options=optionstr)
                if wssdata.ErrorCode != 0:
                    self.logger.error("wind获取因子数据有误，错误代码" + str(wssdata.ErrorCode))
                    return pd.DataFrame()
                resultDf = pd.DataFrame(wssdata.Data, index=wssdata.Fields, columns=wssdata.Codes).T

            elif self.dic_init['data_resource'] == 'ifind':
                thscode = ','.join(code_list)
                indicator = 'ths_pb_mrq_stock;ths_pe_ttm_stock;ths_ps_ttm_stock;ths_chg_ratio_m_stock;ths_compre_rating_value_stock;ths_free_float_mv_stock;ths_beta_24m_stock'
                optionstr = '%s;%s,100;%s,100;%s,100;%s,30;%s;%s' % (
                    trade_date, trade_date, trade_date, trade_date, trade_date, trade_date, trade_date)

                ths_data = THS_BasicData(thsCode=thscode, indicatorName=indicator, paramOption=optionstr, outflag=False)
                if ths_data['errorcode'] != 0:
                    self.logger.error("同花顺获取因子数据错误，请检查:%s" % ths_data['errmsg'])
                    return pd.DataFrame()
                resultDf = THS_Trans2DataFrame(ths_data).set_index(keys='thscode')
            resultDf.rename(columns=name_dic, inplace=True)

            df_list = []
            for col in resultDf:
                temp_df = pd.DataFrame(resultDf[col].values, index=resultDf.index, columns=['factor_value'])
                temp_df['update_time'] = trade_date
                temp_df['stock_code'] = resultDf.index.tolist()
                temp_df['factor_name'] = col
                df_list.append(temp_df)
            total_fa_df = pd.concat(df_list, axis=0, sort=True)
            self.GetDataToMysqlDemo.GetMain(total_fa_df, 'stock_factor_month_value')
            self.logger.info("存储日期%s因子数据成功！" % trade_date)

    def get_factor_value(self, code_list=[], factor_list=[], get_date='2019-08-30'):
        # 获取截面因子数据
        sqlStr = "select * from stock_factor_month_value where stock_code in %s and factor_name in %s and update_time='%s'" % (
            str(tuple(code_list)), str(tuple(factor_list)), get_date)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        if not resultDf.empty:
            df_list = []
            for factor, temp_df in resultDf.groupby(by='factor_name'):
                temp = pd.DataFrame(temp_df['factor_value'].values, index=temp_df['stock_code'], columns=[factor])
                df_list.append(temp)
            resultDf = pd.concat(df_list, sort=True, axis=1)
        return resultDf

    def get_fund_report_data(self, fund_code='100053.OF', start_date='2011-01-30', end_date='2019-12-31'):
        '''
        获取基金的年半，半年报详细持仓数据
        :param fund_code:
        :param start_date:
        :param end_date:
        :return:
        '''
        wdays = w.tdays(beginTime=start_date, endTime=end_date, options="Days=Alldays;Period=S")
        total_date_list = wdays.Data
        total_date_list = [datetime.strftime(date_data, "%Y-%m-%d") for date_data in total_date_list[0]]
        total_date_list = [date_data for date_data in total_date_list if date_data[-5:] in ['06-30', '12-31']]
        GetFundFinanceReportDataDemo = GetFundFinanceReportData()
        result_df = GetFundFinanceReportDataDemo.get_fund_stock_info(third_conn=w, engine=self.engine,
                                                                     fund_code=fund_code,
                                                                     total_date_list=total_date_list)
        return result_df

    def get_stock_industry(self, industry_flag='证监会', industryType=1, code_list=['300033.SZ', '600584.SH'],
                           tradeDate='20200519'):
        '''
        获取股票所属的行业名称
        :param code_list:
        :return:
        '''
        tradeDate = datetime.today().strftime("%Y%m%d")
        option_str = "tradeDate=%s;industryType=%s" % (tradeDate, industryType)
        name_str = "%s行业(%s级)" % (industry_flag, industryType)
        if industry_flag == '证监会':
            filds_str = 'industry_csrc12_n'
        elif industry_flag == '中证':
            filds_str = 'industry_csi'
        elif industry_flag == '中信':
            filds_str = 'industry_citic'
        elif industry_flag == '申万':
            filds_str = 'industry_sw'
            option_str = "industryType=%s" % industryType
        elif industry_flag == 'AMAC':
            name_str = 'AMAC行业'
            filds_str = 'indexname_AMAC'
            option_str = "tradeDate=%s" % tradeDate
        wss_data = w.wss(codes=code_list, fields=filds_str, options=option_str)
        if wss_data.ErrorCode != 0:
            self.logger.error("wind获取股票所属行业分类数据有误，错误代码" + str(wss_data.ErrorCode))
            return pd.DataFrame()
        resultDf = pd.DataFrame(wss_data.Data, index=[name_str], columns=wss_data.Codes).T
        return resultDf

    def get_index_daily_data(self, fields, filed_name, code_list=['000300.SH', '000016.SH'], start_date='2020-01-01',
                             end_date='2020-08-12'):
        '''
        :param code_list:多维度,fiels一维度，
        :return:
        '''
        if (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d") <= start_date:
            self.logger.info("行情数据起止时间间隔不够，不用补全。startdate: %s;enddate: %s" % (start_date, end_date))
            return

        wsd_data = w.wsd(codes=code_list, fields=fields, beginTime=start_date, endTime=end_date, options='')
        if wsd_data.ErrorCode != 0:
            self.logger.error("获取行情数据有误，错误代码" + str(wsd_data.ErrorCode))
            return pd.DataFrame()
        tempDf = pd.DataFrame(wsd_data.Data, index=wsd_data.Codes, columns=wsd_data.Times).T
        tempDf.dropna(inplace=True)
        dateList = [dateStr.strftime("%Y-%m-%d") for dateStr in tempDf.index.tolist()]

        df_list = []
        for index_code in tempDf.columns.tolist():
            temp_index_df = pd.DataFrame(tempDf[index_code].values, index=dateList, columns=['factor_value'])
            temp_index_df['index_code'] = index_code
            temp_index_df['factor_get_str'] = fields
            temp_index_df['update_time'] = dateList
            temp_index_df['record_time'] = datetime.today().strftime("%Y-%m-%d")
            temp_index_df['factor_name'] = filed_name
            df_list.append(temp_index_df)
        result_df = pd.concat(df_list, axis=0, sort=True)
        self.GetDataToMysqlDemo.GetMain(result_df, 'index_daily_data')


if __name__ == '__main__':
    GetDataTotalMainDemo = GetDataTotalMain('wind')
    # GetDataTotalMainDemo.get_index_constituent()
    # GetDataTotalMainDemo.get_tradeday('2019-01-01', '2020-02-02', period='D')
    # GetDataTotalMainDemo.get_stock_month_to_MySql(code_list=['300033.SZ', '600999.SH'])
    # GetDataTotalMainDemo.get_factor_value(code_list=['300033.SZ', '600999.SH'],
    #                                       factor_list=['MKT_FREESHARES', 'PE_TTM'])
    # GetDataTotalMainDemo.get_fund_report_data()
    # GetDataTotalMainDemo.get_stock_industry()
    GetDataTotalMainDemo.get_hq_data(code='000300.SH',code_style='index_daily',dic_param={'fields':'pe_ttm', 'filed_name':'PE值'},start_date='2020-01-01',end_date='2020-08-11')
