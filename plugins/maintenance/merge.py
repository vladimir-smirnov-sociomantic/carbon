import os
import json
from carbon.conf import settings, read_writer_configs
import ceres
#######################################################
# Put your custom aggregation logic in this function! #
#######################################################

read_writer_configs()
try:
  ceres.MAX_SLICE_GAP = int(settings['ceres']['MAX_SLICE_GAP'])
except KeyError:
  pass

def node_found(node):
  metadata = node.readMetadata()

  if not node.slices:
    return

  slices = {}
  for slice in sorted(node.slices, key=lambda x: x.endTime)[:-1]:
    slices.setdefault(slice.timeStep, []).append(slice)
  do_merge(node, slices)

def do_merge(node, slices):
  for (precision,sliceList) in slices.iteritems():
    if not sliceList:
      continue

    sliceList.sort(key=lambda x: (x.startTime, -x.endTime))
    sliceListIterator = iter(sliceList)

    mergeToSlice = next(sliceListIterator)
    try:
      while True:
        nextSlice           = next(sliceListIterator)
        nextSliceEndTime    = nextSlice.endTime
        mergeToSliceEndTime = mergeToSlice.endTime
        # can't merge
        if (nextSlice.startTime - mergeToSliceEndTime) * ceres.DATAPOINT_SIZE > ceres.MAX_SLICE_GAP * precision:
          mergeToSlice = nextSlice
          continue

        # merge slices
        if nextSlice.startTime < mergeToSliceEndTime:
          try:
            slicePoints = nextSlice.read(nextSlice.startTime, min(nextSliceEndTime, mergeToSliceEndTime))
            print "update %d (%d not none): %s -> %s" % (len(slicePoints), len([p for p in slicePoints if p[1] is not None]), str(nextSlice), str(mergeToSlice))

            updatePoints = []
            for point in slicePoints:
              if point[1] is not None:
                updatePoints.append(point)
                continue

              if updatePoints:
                mergeToSlice.write(updatePoints)
                updatePoints = []

            if updatePoints:
              mergeToSlice.write(updatePoints)
          except ceres.NoData:
            pass

        try:
          slicePoints = nextSlice.read(max(nextSlice.startTime, mergeToSliceEndTime), nextSliceEndTime)
          print "append %d (%d not none): %s -> %s" % (len(slicePoints), len([p for p in slicePoints if p[1] is not None]), str(nextSlice), str(mergeToSlice))

          updatePoints = []
          for point in slicePoints:
            if point[1] is not None:
              updatePoints.append(point)
              continue

            if updatePoints:
              mergeToSlice.write(updatePoints)
              updatePoints = []

          if updatePoints:
            mergeToSlice.write(updatePoints)

        except ceres.SliceGapTooLarge:
          nextSliceEndTime = updatePoints[0][0]
        except ceres.NoData:
          pass

        try:
          nextSlice.deleteBefore(nextSliceEndTime)
        except ceres.SliceDeleted:
          pass
    except StopIteration:
      pass

