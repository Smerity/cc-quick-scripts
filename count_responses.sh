#!/bin/bash
# Returns total responses on stdout
# Returns total bytes read on stderr
# Naive testing showed wc --bytes faster than dd or pv (with pv not being 100% accurate)
#
zcat /dev/stdin | tee >(wc --bytes | awk '{ printf "Bytes:\t" $1 "\n" }' > /dev/stderr) | grep "WARC-Type: response" | wc -l | awk '{ printf "Responses:\t" $1 "\n" }' > /dev/stdout
