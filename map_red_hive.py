#!/usr/bin/python

import os
import commands
import datetime, calendar
import time
import sys
import imp
import string
import MySQLdb

from types import *
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from optparse import OptionParser


#**************************************************************************************************************
if len(sys.argv)==1:
        yesterday=time.strftime('%Y%m%d',time.localtime(time.time()-24*60*60))
        a=['%s' % yesterday]
elif len(sys.argv)==2:
        a=['%s' % sys.argv[1]]
else:
        sys.argv
        type(sys.argv)
        month=int(sys.argv[1][4:6])
        type(sys.argv)
        s='%s' % sys.argv[1]
        month=int(s[4:6])
        mdays=calendar.mdays[month]
        print 'mdays=%s' % (mdays)
        smdays='%s' % (mdays)
        print 'smdays=%s' % (smdays)
        month_end=int('%s%s' % (s[0:6],smdays))+1
        s2='%s' % sys.argv[2]
        first_day='01'
        month_start=int('%s%s' % (s2[0:6],first_day))
        int(sys.argv[2])-int(sys.argv[1])
        start_date=int(sys.argv[1])
        end_date=int(sys.argv[2])
        if s[0:6]==s2[0:6]:

                a=range(start_date,end_date+1)
        else:
                a1=range(start_date,month_end)
                a2=range(month_start,end_date+1)
                a=a1+a2

print a
#***********************************************************************************************************************

print "******************************************************************************************************************"
hive_ip='xxx.xxx.xxx.xxx'
hive_port=xxxxx
database="xxx"
table=""
hadoop="path/hadoop/hadoop-0.20.203.0"
map="path/mdaily_map.py"
red="path/mdaily_red.py"
input="path"
output="path"
local_output="path"
print "*******************************************************************************************************************"

try:
		transport = TSocket.TSocket('%s' % hive_ip,hive_port) 
        transport = TTransport.TBufferedTransport(transport)
		protocol = TBinaryProtocol.TBinaryProtocol(transport)

        client = ThriftHive.Client(protocol)
        transport.open()
   
        client.execute('show databases')
        dbs=client.fetchAll()
        print dbs
	
		table_filed="stat_date string,ver string,gid string,field string,uid string,count bigint"
        #client.execute('CREATE DATABASE database' % (database))
        client.execute('use %s' % (database))
		#client.execute('drop table %s_%s' % (database,yesterday))

        for the_date in a:
		
		before_rmr_hdfs="/opt/modules/hadoop/hadoop-0.20.203.0/bin/hadoop fs -rmr %s/%s" % (output,the_date)
		print before_rmr_hdfs
		commands.getstatusoutput(before_rmr_hdfs)
		
		run_map_red="%s/bin/hadoop jar %s/contrib/streaming/hadoop-streaming-0.20.203.0.jar -file %s  -file %s  -mapper %s -reducer %s -inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat -input %s/%s -output %s/%s" % (hadoop,hadoop,map,red,map,red,input,the_date,output,the_date)
		print run_map_red
		t=commands.getstatusoutput(run_map_red)
		print t
		
		copytolocal="%s/bin/hadoop fs -copyToLocal %s/%s  %s" % (hadoop,output,the_date,local_output)
		print copytolocal
		commands.getstatusoutput(copytolocal)
		
		rmr_hdfs="%s/bin/hadoop fs -rmr %s/%s/*" % (hadoop,output,the_date)
		print rmr_hdfs
		commands.getstatusoutput(rmr_hdfs)

		lzop="/usr/local/bin/lzop -U -9  %s/%s/part-00000" % (local_output,the_date)
		print lzop
		commands.getstatusoutput(lzop)
		
		copyfromlocal="%s/bin/hadoop fs -copyFromLocal %s/%s/part-00000.lzo  %s/%s" % (hadoop,local_output,the_date,output,the_date)
		print copyfromlocal
		commands.getstatusoutput(copyfromlocal)
		
		rm_local="rm -rf %s/%s" % (local_output,the_date)
		print rm_local
		commands.getstatusoutput(rm_local)

		yesterday=the_date
		client.execute('drop table %s_%s' % (database,yesterday))
		hsql='create external table IF NOT EXISTS %s.%s_%s (%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY ","  STORED AS INPUTFORMAT "com.hadoop.mapred.DeprecatedLzoTextInputFormat" OUTPUTFORMAT "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat" LOCATION "%s/%s/"' % (database,database,yesterday,table_filed,output,yesterday)
		print hsql
		client.execute(hsql)
		
		client.execute('show tables')		
	        tables=client.fetchAll()
        	print tables
		
except Thrift.TException, tx:
    print '%s' % (tx.message)


