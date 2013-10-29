#!/usr/bin/python -u
import socket
import sys
import time
import cmath

if len(sys.argv) > 1:
    target = sys.argv[1]
else:
    print "need target host!"
    sys.exit(1)

g_start_time = time.time()
step = 100
time_step = 90

size = [500,1000]
count = 0
while 1:
    count += 1
    ts = "%d" % time.time() 
    start_t = time.time()
    for i in xrange(int(size[0])):
        out = ""
        for j in xrange(size[1]):
            out += "test.test%d.test%d %s %s\n" % (i,j,ts,ts)
        out += "\n\n"
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((target, 2024))
            s.send(out)
            s.close()
        except:
            pass

    if count >= time_step:
        size[0] += step
        print "added %d, new size is %s" % (step, size)
        count = 0

    delay = time.time() - start_t
    print "Sending %d took %f" % (size[0]*size[1],time.time() - start_t)
    if delay < 60:
        time.sleep(60 - delay)
        print "slept for %f" % delay
        speed = size[0]*size[1]/delay
        print "speed %f" % speed
    else:
        print "overtime for %f " % (delay - 60)
        speed = size[0]*size[1]/delay
        overflow = (delay - 60) * speed
        print "speed %f, overflow %f" % (speed, overflow)
        

        
