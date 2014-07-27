import os
import sys
###
from prettyplotlib import plt
###
import boto
conn = boto.connect_s3(anon=True)
pds = conn.get_bucket('aws-publicdatasets')

# Get all segments
target = 'CC-MAIN-2014-23'
segments = list(pds.list('common-crawl/crawl-data/{}/segments/'.format(target), delimiter='/'))
# Record the total size and all file paths for the segments
files = dict(warc=[], wet=[], wat=[], segment=[x.name for x in segments])
size = dict(warc=[], wet=[], wat=[])

# Traverse each segment and all the files they contain
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {}'.format(i, len(segments)))
  for ftype in ['warc', 'wat', 'wet']:
    for f in pds.list(segment.name + ftype + '/'):
      files[ftype].append(f.name)
      size[ftype].append(f.size)
sys.stderr.write('\n')

# Write total size and file paths to files
prefix = 'crawl_stats/{}/'.format(target)
if not os.path.exists(prefix):
  os.makedirs(prefix)
###
f = open(prefix + 'crawl.size', 'w')
for ftype, val in size.items():
  f.write('{}\t{}\n'.format(ftype, sum(val)))
f.close()
###
for ftype in files:
  f = open(prefix + '{}.path'.format(ftype), 'w')
  for fn in files[ftype]:
    f.write(fn + '\n')
  f.close()
###
# Kid friendly stats (i.e. console)
for ftype, fsize in size.items():
  print '{} files contain {} bytes over {} files'.format(ftype.upper(), sum(fsize), len(files[ftype]))
###
# Note: you might want to run `gzip *` before upload

###
# Plot
for ftype, fsize in size.items():
  plt.hist(fsize, bins=50)
  plt.xlabel('Size (bytes)')
  plt.ylabel('Count')
  plt.title('Distribution for {}'.format(ftype.upper()))
  plt.savefig('crawl_stats/{}_dist.pdf'.format(ftype))
  plt.show(block=True)
