import sys
###
import boto
conn = boto.connect_s3()
pds = conn.get_bucket('aws-publicdatasets')

# Get all segments
target = 'CC-MAIN-2014-23'
segments = list(pds.list('common-crawl/crawl-data/{}/segments/'.format(target), delimiter='/'))
## For testing ...
#class Seg(object):
#  name = 'common-crawl/crawl-data/CC-MAIN-2014-23/segments/1406510280868.21/'
#segments = [Seg()]

# Traverse each segment and all the files they contain
for i, segment in enumerate(segments):
  sys.stderr.write('\rProcessing segment {} of {}'.format(i, len(segments)))
  # Open the directories
  for ftype in ['wat', 'wet']:
    # Open the files
    files = list(pds.list(segment.name + ftype + '/'))
    for fnum, f in enumerate(files):
      k = boto.s3.key.Key(pds)
      k.key = f.name
      k.set_acl('public-read')
      sys.stderr.write('\rProcessing segment {} of {}: {} file {} of {}\t\t\t'.format(i, len(segments), ftype, fnum, len(files)))
sys.stderr.write('\n')
