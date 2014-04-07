#!/usr/bin/python
import sys, os, time, posixfile
from ceres import getTree
from multiprocessing import Pool
from os.path import join, exists
from optparse import OptionParser

def log_msg(lvl, msg):
    print "%s [%s] %s" % (time.ctime(), lvl.lower(), msg)

lock_file = '/var/tmp/simple-rollup.lock'
# Make carbon imports available for some functionality
root_dir = "/var/lib/graphite"
sys.path.append(join(root_dir, "plugins/maintenance"))
sys.path.append(join(root_dir,"lib"))

try:
  import carbon
except ImportError:
  log_msg("fatal","Failed to import carbon, specify your installation location "
         "with the GRAPHITE_ROOT environment variable.")
  sys.exit(1)

# set up config
from carbon.conf import settings, load_storage_rules
settings.use_config_directory('/var/lib/graphite/conf/carbon-daemons/carbon-writer-st01')
from rollup_ng import node_found

parser = OptionParser()
parser.add_option('--root', default='/var/lib/graphite/storage/ceres/', help="Specify were to perform maintenance (default: /var/lib/graphite/storage/ceres/)")
parser.add_option('--metric', help="Specify path to the metric you want to rollup")

options, args = parser.parse_args()
root = options.root

if __name__ == '__main__':
  lock_timeout = 60
  got_lock = 0
  while lock_timeout:
    try:
      lock = posixfile.open(lock_file, 'w')
      lock.lock('w')
      got_lock = 1
      break
    except IOError, e:
      if e[0] == 11:
        lock_timeout = lock_timeout - 1 
        time.sleep(1)
      else:
        log_msg("fatal", "can't get lock, reason: %s" % e[1])
        sys.exit(1)
    except:
      log_msg("fatal", "failed to get lock for some unknown reason")
      sys.exit(1)

  if not got_lock:
    log_msg("fatal", "Failed to get lock for 60s")
    sys.exit(1)

  tree = getTree(root)
  if tree is None:
    log_msg("fatal", "%s is not inside a CeresTree" % root)
    sys.exit(1)

  log_msg("info", "Starting rollup")
  nodes_found = 0
  exec_time = time.time()
  if options.metric:
    node_found(options.metric, root)
  else:
    proc_pool = Pool(processes = 4)
    for current_dir, subdirs, files in os.walk(root):
      for subdir in subdirs:
        if subdir == '.ceres-tree':
          continue
  
        path = join(current_dir, subdir)
  
        if os.listdir(path):
  
          if exists( join(path, '.ceres-node') ):
            proc_pool.apply_async(node_found, (path, root,))
            nodes_found += 1
  
    proc_pool.close()
    proc_pool.join()
    log_msg("info", "found %s nodes" % nodes_found)
  log_msg("info", "All work is done")
  


