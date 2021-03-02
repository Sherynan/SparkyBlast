#!/bin/bash
ResFile=${1%.*}".csv"
echo "Convert "$1" to "$ResFile

tail -n +2 $1 | awk -F " ; " 'BEGIN{ cont=0 } { count = count + 1; print $0, "\t", count , "\t", length($0) }' > $ResFile

# gzip -d -c GRCh38_latest_genomic_2P.fna.gz | tail -n +2 $1 | awk -F " ; " 'BEGIN{ cont=0 } { count = count + 1; print $0, "\t", count , "\t", length($0) }' | gzip > GRCh38_latest_genomic_2P.csv.gz
