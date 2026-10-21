#!/bin/bash
S=$1; P=$2
while read pkg t; do echo "$pkg $t $S ${S}__$(echo $t | awk -F:: '{print $(NF-2)"__"$NF}')"; done < $TMPDIR/tests.txt | xargs -P $P -L 1 $TMPDIR/run1.sh
echo ALLDONE $S
