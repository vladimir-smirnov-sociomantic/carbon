import time
from ceres import CeresSlice, SliceDeleted

#######################################################
# Put your custom aggregation logic in this function! #
#######################################################
def aggregate(node, datapoints):
  "Put your custom aggregation logic here."
  values = [value for (timestamp,value) in datapoints if value is not None]
  metadata = node.readMetadata()
  method = metadata.get('aggregationMethod', 'avg')

  if method in ('avg', 'average'):
    return float(sum(values)) / len(values) # values is guaranteed to be nonempty

  elif method == 'sum':
    return sum(values)

  elif method == 'min':
    return min(values)

  elif method == 'max':
    return max(values)

  elif method == 'median':
    values.sort()
    return values[ len(values) / 2 ]


def node_found(node):
  archives = []
  t = int( time.time() )
  metadata = node.readMetadata()

  for (precision, retention) in metadata['retentions']:
    archiveEnd =  t - (t % precision)
    archiveStart = archiveEnd - (precision * retention)
    t = archiveStart
    archives.append({
      'precision' : precision,
      'retention' : retention,
      'startTime' : archiveStart,
      'endTime' : archiveEnd,
      'slices' : [s for s in node.slices if s.timeStep == precision]
    })

  for i, archive in enumerate(archives):
    if i == len(archives) - 1:
      do_rollup(node, archive, None)
    else:
      do_rollup(node, archive, archives[i+1])



def do_rollup(node, fineArchive, coarseArchive):
  start_t = time.time()
  overflowSlices = [s for s in fineArchive['slices'] if s.startTime < fineArchive['startTime']]
  if not overflowSlices:
    return
  if coarseArchive is None: # delete the old datapoints
    for slice in overflowSlices:
      try:
        slice.deleteBefore(fineArchive['startTime'])
      except SliceDeleted:
        pass

  else:
    # some profiling
    time_stat = {}

    start_t_tmp = time.time()
    
    overflowDatapoints = []
    for slice in overflowSlices:
      datapoints = slice.read(slice.startTime, fineArchive['startTime'])
      overflowDatapoints.extend( list(datapoints) )
    
    time_stat['extended'] = time.time() - start_t_tmp
    start_t_tmp = time.time()

    overflowDatapoints.sort()
    fineStep = fineArchive['precision']
    coarseStep = coarseArchive['precision']
    deletePriorTo = coarseArchive['startTime'] + (coarseStep * coarseArchive['retention'])
    
    metadata = node.readMetadata()
    xff = metadata.get('xFilesFactor')

    time_stat['sorted'] = time.time() - start_t_tmp
    start_t_tmp = time.time()

    # We define a window corresponding to exactly one coarse datapoint
    # Then we use it to select datapoints for aggregation

    # oldest timestamp in overflowDatapoints array is start point for moving through retention
    try:
        oldest_ts = overflowDatapoints[0][0]
    except:
        oldest_ts =0
    # calculate the start point for retentions
    start_p = int((oldest_ts - coarseArchive['startTime'])/coarseStep)
    
    # profiling variables
    start_count = 0
    fine_dp = 0

    for i in range(start_p, coarseArchive['retention']):
    #for i in range(coarseArchive['retention']):
      start_count += 1
      start_t_in = time.time()
      windowStart = coarseArchive['startTime'] + (i * coarseStep)
      windowEnd = windowStart + coarseStep
      fineDatapoints = [d for d in overflowDatapoints if d[0] >= windowStart and d[0] < windowEnd]
      time_stat.setdefault(1, []).append(time.time() - start_t_in)

      if fineDatapoints:
        time_stat.setdefault(6, []).append(time.time() - start_t_in)
        fine_dp += 1
        start_t_in = time.time()
        knownValues = [value for (timestamp,value) in fineDatapoints if value is not None]
        if not knownValues:
          continue
        knownPercent = float(len(knownValues)) / len(fineDatapoints)
        if knownPercent < xff:  # we don't have enough data to aggregate!
          continue
        time_stat.setdefault(2, []).append(time.time() - start_t_in)
        start_t_in = time.time()
        
        coarseValue = aggregate(node, fineDatapoints)
        coarseDatapoint = (windowStart, coarseValue)
        fineValues = [d[1] for d in fineDatapoints]

        time_stat.setdefault(3, []).append(time.time() - start_t_in)
        start_t_in = time.time()
        written = False
        for slice in coarseArchive['slices']:
          if slice.startTime <= windowStart and slice.endTime >= windowStart:
            slice.write([coarseDatapoint])
            written = True
            break

        time_stat.setdefault(4, []).append(time.time() - start_t_in)
        start_t_in = time.time()
          # We could pre-pend to an adjacent slice starting after windowStart
          # but that would be much more expensive in terms of I/O operations.
          # In the common case, append-only is best.

        if not written:
          newSlice = CeresSlice.create(node, windowStart, coarseStep)
          newSlice.write([coarseDatapoint])
          coarseArchive['slices'].append(newSlice)
          deletePriorTo = min(deletePriorTo, windowStart)
        time_stat.setdefault(5, []).append(time.time() - start_t_in)

    time_stat['aggregated'] = time.time() - start_t_tmp
    start_t_tmp = time.time()
    # Delete the overflow from the fine archive
    for slice in overflowSlices:
      try:
        slice.deleteBefore( deletePriorTo ) # start of most recent coarse datapoint
      except SliceDeleted:
        pass
    time_stat['overflow_delete'] = time.time() - start_t_tmp
    time_stat['all'] = time.time() - start_t

    out = ""
    keys = [1, 2, 3, 4, 5, 6, 'overflow_delete', 'extended', 'sorted', 'aggregated', 'all']
    for k in keys:
        if not time_stat.has_key(k): continue
        v = time_stat[k]
        if type(v) == list:
            out = "%s, %s: %.3f" % (out, k, sum(v))
        else:
            out = "%s, %s: %.3f" % (out, k, v)
    print "node: %s, profiling - cycles: %d, fine_dp: %d, %s" % (node.fsPath, start_count, fine_dp, out)


