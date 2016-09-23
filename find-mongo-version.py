import subprocess
import sys

print "Message"
mongoclient_status = subprocess.call(["dpkg", "-s", "mongodb-clients"])
print mongoclient_status

conffile = open(sys.argv[1]+'/src/mongoversion.h', 'w')

if (0 == mongoclient_status):
  conffile.write('#define USE_DEFAULT_MONGODB\n\n')
else:
  conffile.write('/*Default MongoDB not found, assume Mongo3 present*/\n\n')

sys.exit(mongoclient_status == 0)


