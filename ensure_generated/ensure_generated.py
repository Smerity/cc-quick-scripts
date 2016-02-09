import sys
###
import boto
conn = boto.connect_s3(anon=True)
pds = conn.get_bucket('aws-publicdatasets')

# Get all segments
# s3://aws-publicdatasets/common-crawl/nutch/segments.20150123/
target = "common-crawl/nutch/segments.20150929/"
segments = list(pds.list(target, delimiter='/'))
print 'Total of {} segments'.format(len(segments))

good, bad = 0, 0

d = {}
seg_sizes = []
dead_segs = set()
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {}'.format(i, len(segments)))
  fetchlists = list(pds.list(segment.name + 'crawl_generate/'))
  if len(fetchlists) != 400:
    bad += 1
    sys.stderr.write('\n')
    sys.stderr.write('{} has {} fetchlists\n'.format(segment.name, len(fetchlists)))
    dead_segs.add(segment.name)
  else:
    good += 1
    seg_size = sum(x.size for x in fetchlists)
    if seg_size not in d:
      d[seg_size] = []
    d[seg_size].append(segment.name)
    seg_sizes.append((segment, seg_size))
sys.stderr.write('\n')

seg_sizes = sorted(seg_sizes, key=lambda x: x[1])

print 'Total good segments: {}'.format(good)
print 'Total bad segments: {}'.format(bad)
print 'Total dead segments: {}'.format(len(dead_segs))
print 'Average size: {}'.format(sum(x[1] for x in seg_sizes) / len(seg_sizes))
print 'Unique total sizes for segments: {}'.format(len(set(x[1] for x in seg_sizes)))

# rstrip the segment ends as sometimes we do silly tricks to get the segment name
# i.e. rev | cut -d '/' -f 1 | rev

good_segs = set()
with open('/tmp/good_segs', 'w') as f:
  for size in d:
    good_seg = d[size][-1]
    good_segs.add(good_seg)
    f.write('s3a://aws-publicdatasets/{}\n'.format(good_seg.rstrip('/')))

all_segs = set(x[0].name for x in seg_sizes)
bad_segs = all_segs - good_segs | dead_segs
with open('/tmp/bad_segs', 'w') as f:
  for seg in bad_segs:
    f.write('s3://aws-publicdatasets/{}\n'.format(seg.rstrip('/')))
