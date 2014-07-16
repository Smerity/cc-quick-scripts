import os
import sys
###
import boto
conn = boto.connect_s3(anon=True)
pds = conn.get_bucket('aws-publicdatasets')

# Get all segments
segments = list(pds.list('common-crawl/crawl-data/CC-MAIN-2014-15/segments/', delimiter='/'))
# Record the total size and all file paths for the segments
files = dict(warc=[], wet=[], wat=[], segments=[x.name for x in segments])
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
if not os.path.exists('./crawl_stats/'):
  os.makedirs('./crawl_stats')
###
f = open('crawl_stats/crawl_size.txt', 'w')
for ftype, val in size.items():
  f.write('{}\t{}\n'.format(ftype, val))
f.close()
###
for ftype in files:
  f = open('crawl_stats/{}_list.txt'.format(ftype), 'w')
  for fn in files[ftype]:
    f.write(fn + '\n')
  f.close()
###
# Kid friendly stats (i.e. console)
for ftype, fsize in size.items():
  print '{} files contain {} bytes over {} files'.format(ftype, fsize, len(files[ftype]))
