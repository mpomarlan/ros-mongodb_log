#! /usr/bin/env python
import re
import sys



if(2 > len(sys.argv)):
  print("Too few parameters. Usage:\nlogreg.py <filename>")

print("Will now process %s\n" % sys.argv[1])

infile = open(sys.argv[1], 'r')
data = infile.read()
infile.close()

addDatesRE = re.compile('\"stamp\"\s*:\s*\{\s*\"\$numberLong\"\s*:\s*\"\s*(?P<number>\d*)\s*\"\s*\}')
data = addDatesRE.sub('"stamp" : {"$date" : \g<number>}', data)

repLIntsRE = re.compile('\{\s*\"\$numberLong\"\s*:\s*\"\s*(?P<number>\d*)\s*\"\s*\}')
data = repLIntsRE.sub('\g<number>', data)

outfile = open(sys.argv[1], 'w')
outfile.write("%s" % data)
outfile.close()
