# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
 wind/ifind 账号登录初始化
'''

import mylog as mylog
from WindPy import *
import pandas as pd
from GetAndSaveWindData.MysqlCon import MysqlCon
from iFinDPy import *

class ClientWindIfindInit:
    def __init__(self,data_source='ifind'):
        self.logger = mylog.logger

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
            w.start()
        return flag