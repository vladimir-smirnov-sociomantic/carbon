#!/usr/bin/python
import sys, os, time
from ceres import getTree
from multiprocessing import Pool
from os.path import join, exists

# Make carbon imports available for some functionality
root_dir = "/var/lib/graphite"
sys.path.append(join(root_dir, "plugins/maintenance"))
sys.path.append(join(root_dir,"lib"))

try:
  import carbon
except ImportError:
  print ("Failed to import carbon, specify your installation location "
         "with the GRAPHITE_ROOT environment variable.")
  sys.exit(1)

# set up config
from carbon.conf import settings, load_storage_rules
settings.use_config_directory('/var/lib/graphite/conf/carbon-daemons/carbon-writer-st01')
from rollup_ng import node_found

if len(sys.argv) > 1:
  root = sys.argv[1]
else:
  root = '/var/lib/graphite/storage/ceres'

if __name__ == '__main__':
  tree = getTree(root)
  if tree is None:
    print "%s is not inside a CeresTree" % root
    sys.exit(1)

  print "Starting rollup"
  nodes_found = 0
  exec_time = time.time()
  proc_pool = Pool(processes = 32)
  for current_dir, subdirs, files in os.walk(root):
    for subdir in subdirs:
      if subdir == '.ceres-tree':
        continue

      path = join(current_dir, subdir)

      if os.listdir(path):

        if exists( join(path, '.ceres-node') ):
          node_params = (path, root)	    
          proc_pool.apply_async(node_found, (path, root,))
          nodes_found += 1

  proc_pool.close()
  proc_pool.join()
  print "found %s nodes" % nodes_found
  print "All work is done"
  


