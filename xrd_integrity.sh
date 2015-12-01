#!/bin/bash

# How this script works:
# It looks in all the xrootd cache directories on the gpatlas cluster.
# From each node, it runs rsync to check against EOS (mounted on some lxplus node)
# for "integrity" of the dataset.

# "Integrity" is defined to mean only that
# 1) The each EOS file exists on at least one xrootd node 
# 2) The FILE SIZE of each file is identical to the EOS version
# 3) Files which are not on EOS do not exist on any xrootd node

# NB: This means in particular that files that have changed in
# content but not size will go undetected! This is because it
# takes a very long time to compute the checksum of every file
# in such a large dataset.
# Rsync gets around this by comparing size AND modification date;
# however, since the files are transferred downstream using EOS,
# the modification date is not preserved.

TAG="CxAOD_00-18-01_inclusive"

XRD_CACHE_BASE="/atlas/local/cshimmin/complete"

EOS_CACHE_BASE="eos/atlas/atlascerngroupdisk/phys-higgs/HSG5/Run2/VH"

HOSTS="c-12-15 c-12-19 c-12-23 c-12-27 c-12-31 c-12-35 c-12-39"

show_usage() {
    echo "$0 lxplus_node region"
    echo "   lxplus_node  -- the lxplus node to connect to w/ rsync (must have eos mounted in ~/eos)"
    echo "   region       -- the derivation region (1,2,4)"
}

server=$1
if [ -z "$server" ]; then
    show_usage
    echo "No lxplus server specified" >&2
    exit 1
fi

region=$2
if [ -z "$region" ]; then
    show_usage
    echo "No region specified (1,2,4)" >&2
    exit 1
fi


if [ $region -eq 1 ]; then
    derivation=HIGG5D1_13TeV
elif [ $region -eq 2 ]; then
    derivation=HIGG5D2_13TeV
elif [ $region -eq 4 ]; then
    derivation=HIGG2D4_13TeV
else
    show_usage
    echo "Invalid region specified: '$region'" >&2
    exit 1
fi

echo "Region:  $derivation"
echo "Tag:     $TAG"
echo

# save all the rsync output to a temp file
ftemp=$(mktemp)
for host in $HOSTS; do
    echo "Running on $host" >&2

    # do rsync *dry run* in itemized/verbose mode to get a list of all files that differ, and why/how they differ
    ssh $host "cd /scratch/${XRD_CACHE_BASE}/${derivation} && rsync --prune-empty-dirs --delete --size-only --dry-run -iav $server:~/${EOS_CACHE_BASE}/${derivation}/${TAG} ."
    
done > $ftemp

# now process the temp file to figure out the differences

echo "=== Missing ==="
# ">+++++++" means the file doesn't exist locally.
# So, here we filter for files that are missing on all 7 machines
mfile=d${region}_missing.list
grep "+++" $ftemp | sort | uniq -c | grep "  7" > $mfile
echo "output suppressed..."
echo

echo "=== Size Mismatch ==="
# Anything starting with ">f..." means that rsync wants to download
# changes to a file (because the size didn't match the upstream).
# Filter out the totally new files and then show remaining file differences.
sfile=d${region}_size.list
grep -v "+++" $ftemp | grep ">" > $sfile
echo "output supressed..."
echo

echo "=== To Delete ==="
# Anything starting with "*deleting" means that rsync didn't
# find the file upstream, and wants to remove it locally.
dfile=d${region}_delete.list
n_delete=0
for line in $(grep "^\*deleting" $ftemp | awk '{ print $2 }' | sort | uniq); do
    n_delete=$((n_delete+1))
    echo "${XRD_CACHE_BASE}/${derivation}/${line}"
done > $dfile

if [ $n_delete -eq 0 ]; then
    echo "No files to delete."
else
    cat $dfile
fi

# clean up the temp file.
rm $ftemp
