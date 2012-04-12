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

try:
	for the_date in a:
        	hadoop="/opt/modules/hadoop/hadoop-0.20.203.0"
        	map="path/xxx_map.py"
        	red="path/xxx_red.py"
        	input="path/%s" % (the_date)
        	output="path/xxx_%s" % (the_date)
		table="xxx"
		
		test="%s/bin/hadoop jar %s/contrib/streaming/hadoop-streaming-0.20.203.0.jar -jobconf mapred.reduce.tasks=21  -jobconf  mapred.job.queue.name=ETL -file  %s  -file %s  -mapper %s -reducer %s -inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat -input %s  -output %s" % (hadoop,hadoop,map,red,map,red,input,output)
		print test
		t=commands.getstatusoutput(test)
		print t
		t=commands.getstatusoutput("%s/bin/hadoop fs -copyToLocal  %s /tmp" % (hadoop,output))
		os.system("sudo cat  %s/part-* > %s/part-00021" % (output,output))		
		print t
		commands.getstatusoutput("%s/bin/hadoop fs -rmr %s" % (hadoop,output))
		yesterday=str(the_date)
		s="mysql -hxxx.xxx.xxx.xxx -uxxx -pxxx -Pxxxx  xxxxx  --local-infile=1 -e \"load data local infile '"+output+"/part-00021' into table "+table+"  FIELDS TERMINATED BY '\ , ' (xxx,xxx,xxx,....,xxx,xxx)\""
		print s	
		os.system(s)	
        	os.system ("rm -rf "+output)
		
except Thrift.TException, tx:
    print '%s' % (tx.message)


