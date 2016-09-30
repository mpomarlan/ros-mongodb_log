import re
import sys

def main(argv):

  if(2 > len(argv)):
    print("Too few parameters. Usage:\nlogreg.py <filename>")

  infile = open(argv[1], 'r')
  data = infile.read()
  infile.close()

  addDatesRE = re.compile('\"stamp\"\s*:\s*\{\s*\"\$numberLong\"\s*:\s*\"\s*(?P<number>\d*)\s*\"\s*\}')
  data = addDatesRE.sub('"stamp" : {"$date" : \g<number>}', data)

  repLIntsRE = re.compile('\{\s*\"\$numberLong\"\s*:\s*\"\s*(?P<number>\d*)\s*\"\s*\}')
  data = repLIntsRE.sub('\g<number>', data)

  outfile = open(argv[1], 'r')
  outfile.write("%s" % data)
  outfile.close()
