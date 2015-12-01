#!/bin/bash

# Delete files and directories listed in the given file

listfile=$1

if [ -z "$listfile" ]; then
    echo "Provide a list of files to delete." >&2
    exit 1
fi

if [ ! -e "$listfile" ]; then
    echo "File not found: $listfile" >&2
    exit 1
fi

# first delete the files
echo "Removing files..."
for f in $(grep -v '\/$' $listfile); do
    echo "Removing $f"
    xrdfs gpatlas2-ib.local rm $f
done

# now delete directories (end with trailing '/').
# note that since xrootd is weird, we should (try) to
# do this 7 times to make sure that the path is removed
# on every node.
for i in {0..6}; do
    echo "Removing directories (pass $((i+1)))..."
    for f in $(grep '\/$' $listfile); do
	xrdfs gpatlas2-ib.local rmdir $f
    done
done
