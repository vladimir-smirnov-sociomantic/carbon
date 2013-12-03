#!/usr/bin/python -u
import socket
import sys
import time
import re
from math import sin
from optparse import OptionParser

DEBUG = 0

def log_msg(lvl,msg):
    if re.match('debug',lvl.lower()):
        if not DEBUG: return
    print "%s [%s] %s" % (time.ctime(), lvl.lower(), msg)
    return

# get cli arguments
parser=OptionParser()
parser.add_option("-r","--rate",action="store",type="string",dest="rate",default="50x1000",help="test rate, multiplication is zise of packet")
parser.add_option("-d","--destination",action="store",type="string",dest="dest", default="bsceres-test04h.yandex.net",help="destination for perf test")
parser.add_option("-p","--port",action="store",type="int",dest="port",default=2024, help="destination port")
parser.add_option("-i","--interval",action="store",type="int",dest="interval",default=10, help="time step for increasing load")
parser.add_option("-s","--step",action="store",type="int",dest="step",default=50, help="step for increasing load")
parser.add_option("--debug",action="store",type="int",dest="debug")
parser.add_option("--duration",action="store",type="int",dest="duration", help="test duration limit, default unlimited")
parser.add_option("--prefix",action="store",type="string",dest="prefix", default="one_min.perf_test", help="metrics perfix")
(options,args)=parser.parse_args()

step = options.step
time_step = options.interval

rate_re = re.compile(r'^([0-9]+)x([0-9]+)$')
m = rate_re.match(options.rate)
if m:
    size = [int(m.group(1)),int(m.group(2))]
else:
    log_msg("fatal","Failed to parse rate %s" % options.rate)
    sys.exit(1)

if options.debug: DEBUG = 1

count = 0
g_count = 0
g_start_time = time.time()

while 1:
    count += 1
    g_count += 1
    ts = "%d" % time.time() 
    start_t = time.time()
    for i in xrange(int(size[0])):
        out = ""
        for j in xrange(size[1]):
            out += "%s.test%d.m%d %s %s\n" % (options.prefix, i, j, sin(float(ts)), ts)
        out += "\n\n"
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((options.dest, options.port))
            s.send(out)
            s.close()
        except:
            log_msg("debug","failed to connect to graphite")

    if count >= time_step:
        size[0] += step
        log_msg("info","added %d, new size is %s" % (step, size))
        count = 0

    delay = time.time() - start_t
    log_msg("debug","Sending %d took %f" % (size[0]*size[1],time.time() - start_t))
    if delay < 60:
        time.sleep(60 - delay)
        log_msg("debug", "slept for %f" % delay)
        speed = size[0]*size[1]/delay
        log_msg("debug", "speed %f" % speed)
    else:
        log_msg("debug", "overtime for %f " % (delay - 60))
        speed = size[0]*size[1]/delay
        overflow = (delay - 60) * speed
        log_msg("debug", "speed %f, overflow %f" % (speed, overflow))

    if options.duration and g_count > options.duration:
        log_msg("info", "ended by time limit %d, current size %s" % (options.duration, size))
        sys.exit(0)
        

        
