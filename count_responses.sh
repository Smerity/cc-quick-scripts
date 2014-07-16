#!/bin/bash
# Saves total response count to responses folder
# Returns total byte count to size folder
# Naive testing showed wc --bytes faster than dd or pv (with pv not being 100% accurate)
#
mkdir -p responses
mkdir -p size
s3cmd get s3://aws-publicdatasets/$1 - | zcat | tee >(wc --bytes > size/`basename $1`) | grep "WARC-Type: response" | wc -l > responses/`basename $1`
# Helpful:
# As there's no good error checking, errors sometimes pop up leaving empty or tiny results
# cat * | grep -E '[0-9]{6,10}' | wc -l
# cat * | grep -E '[0-9]{6,10}' | awk '{ SUM += $1} END { print SUM }'
###
# cat * | grep -E '[0-9]{4,10}' | wc -l
# cat * | grep -E '[0-9]{4,10}' | awk '{ SUM += $1} END { print SUM }'
