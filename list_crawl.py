import os
import sys
###
import boto
conn = boto.connect_s3(anon=True)
pds = conn.get_bucket('aws-publicdatasets')

# Get all segments
segments = list(pds.list('common-crawl/crawl-data/CC-MAIN-2014-15/segments/', delimiter='/'))
# Record the total size and all file paths for the segments
files = dict(warc=[], wet=[], wat=[], segments=segments)
size = dict(warc=0, wet=0, wat=0)

# Traverse each segment and all the files they contain
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {}'.format(i, len(segments)))
  for ftype in ['warc', 'wat', 'wet']:
    for f in pds.list(segment.name + ftype + '/'):
      files[ftype].append(f.name)
      size[ftype] += f.size
sys.stderr.write('\n')

# Write total size and file paths to files
os.makedirs('crawl_stats')
###
print 'Total size in bytes for WARC, WAT and WET:'
print size
f = open('crawl_stats/crawl_size.txt', 'w')
for ftype, val in size.items():
  f.write('{}\t{}\n'.format())
f.close()
###
print 'Writing file lists for WARC, WAT and WET...'
for ftype in files:
  f = open('crawl_stats/{}_list.txt'.format(ftype), 'w')
  for fn in files[ftype]:
    f.write(fn + '\n')
  f.close()
