import sys
debug = True
#debug = false
if debug:
	lzo = 0
else:
	lzo = 1
for line in sys.stdin:
       	try:
		flags = line[1+lzo:-1]     
		str = flags+'\t'+'1'
		print str

	except Exception,e:
		print e
