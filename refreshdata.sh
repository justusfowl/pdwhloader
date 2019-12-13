#!/bin/bash
# Usage: remove all utility bills pdf file password

EXT=ldt
IN_DIR=/mnt/_labor/*
BAD_FILES=/mnt/_labor/_badfiles
DUP_FILES=/mnt/_labor/_dups
ARCHIVE_FILES=/mnt/_labor/_dups

echo "Loading lab files..."

for i in $IN_DIR; do
    if [ "${i}" != "${i%.${EXT}}" ];then

        status_code=$(curl --write-out %{http_code} --silent --output /dev/null -F "file=@$i" localhost:5000/lab/ldt)

        if [ "$status_code" -eq 201 ] ; then
            echo "Status $status_code for $i | Archiving..."
            mv "$i" "$ARCHIVE_FILES"

        elif [ "$status_code" -eq 409 ] ; then
            echo "Status $status_code for $i | Duplicate..."
            mv "$i" "$DUP_FILES"
        else
            echo "Status $status_code for $i | BADFILE..."
            mv "$i" "$BAD_FILES"
        fi
    fi
done

echo "Loading PVS/AIS data..."

curl -H "Content-Type: application/json" --data @/home/administrator/refreshdata.json http://localhost:5000/load
