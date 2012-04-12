#!/usr/bin/python
import sys
lastuid=""
num=0
for line in sys.stdin:
	try:
 		flags=line[:-1].split('\t')
        	uid=flags[0]
        	if lastuid =="":
                	lastuid=uid
                	num=0
        	if lastuid == uid:
                	num +=1
        	if lastuid != uid:
                	print lastuid,num
                	lastuid=uid
                	num = 1
	except Exception,e:
		pass

print lastuid,num
				
