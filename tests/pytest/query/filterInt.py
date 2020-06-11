###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
import taos
from util.log import *
from util.cases import *
from util.sql import *


class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
    
        self.rowNum = 10
        self.ts = 1537146000000       
        
    def run(self):
        tdSql.prepare()

        print("==============step1")
        tdLog.debug(
            "create table st(ts timestamp, num int) tags(id int)")
        tdSql.execute(
            "create table st(ts timestamp, num int) tags(id int)")
        
        #create 10 tables, insert 10 rows for each table
        for i in range(self.rowNum):            
            tdSql.execute("create table if not exist st%d using st tags(%d)" % (i + 1, i + 1))
            for j in range(self.rowNum):                
                tdSql.execute("insert into st%d values(%d, %d)" % (i + 1, self.ts + j + 1, i * 10 + j + 1))
        
        # > for int type on column
        tdSql.query("select * from st where num > 10")
        tdSql.checkRows(90)

        # >= for int type on column
        tdSql.query("select * from st where num >= 10")
        tdSql.checkRows(91)

        # = for int type on column
        tdSql.query("select * from st where num = 10")
        tdSql.checkRows(1)

        # < for int type on column
        tdSql.query("select * from st where num < 10")
        tdSql.checkRows(9)

        # <= for int type on column
        tdSql.query("select * from st where num <= 10")
        tdSql.checkRows(10)
       
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
