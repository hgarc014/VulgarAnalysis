#!/bin/bash

if  [ -z $1 ] || [ -z $2 ] || [ -z $3 ] ;then
    echo "./runCrawlerExecutable.sh <Tweets> <File Sizes (mb)> <outputdir>"
    echo "OR..."
    echo "./runCrawlerExecutable.sh <Tweets> <File Sizes (mb)> <outputdir> <save Every # Tweets>"
    exit
fi


if [ -d "$3" ];then
    START=$(date +%s)
    java -jar jars/crawlTweets.jar $1 $2 $3 $4 || exit

    END=$(date +%s.%N)
    dt=$(echo "$END - $START" | bc)
    dd=$(echo "$dt/86400" | bc)
    dt2=$(echo "$dt-86400*$dd" | bc)
    dh=$(echo "$dt2/3600" | bc)
    dt3=$(echo "$dt2-3600*$dh" | bc)
    dm=$(echo "$dt3/60" | bc)
    ds=$(echo "$dt3-60*$dm" | bc)

    printf "Total runtime: %d:%02d:%02d:%02.4f\n" $dd $dh $dm $ds
else
    echo "seems you provided an invalid directory"
fi
