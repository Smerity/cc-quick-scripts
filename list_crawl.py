import sys
###
import boto
conn = boto.connect_s3(anon=True)
pds = conn.get_bucket('aws-publicdatasets')

segments = list(pds.list('common-crawl/crawl-data/CC-MAIN-2014-10/segments/', delimiter='/'))
files = dict(warc=[], wet=[], wat=[], segments=segments)
size = dict(warc=0, wet=0, wat=0)

total_bytes = 0
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {}'.format(i, len(segments)))
  for ftype in ['warc', 'wat', 'wet']:
    for f in pds.list(segment.name + ftype + '/'):
      files[ftype].append(f.name)
      size[ftype] += f.size
sys.stderr.write('\n')

print 'Total size in bytes for WARC, WAT and WET:'
print size
f = open('crawl_size.txt', 'w')
f.write(repr(size) + '\n')
f.close()

print 'Writing file lists for WARC, WAT and WET...'
for ftype in files:
  f = open('{}_list.txt'.format(ftype), 'w')
  for fn in files[ftype]:
    f.write(fn + '\n')
  f.close()
print 'Completed'
