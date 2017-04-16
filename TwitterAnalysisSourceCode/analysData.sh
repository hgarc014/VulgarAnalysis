#!/bin/bash


if [ -z $1 ] || [ -z $2 ] || [ -z $3 ];then
    echo './analysData.sh newDir tweetDir output'
fi

newDir=$1
tweetDir=$2
output=$3

states="states.csv"
badWords="badWords.csv"
goodWords="goodWords.csv"

hadoopOutTxt="HadoopOut.txt"

java -jar score.jar $newDir $tweetDir $states $badWords $goodWords

hadoop jar computeState.jar $newDir $output

hadoop fs -cat $output/part-r-00000 > $hadoopOutTxt 

java -jar insertState.jar $hadoopOutTxt 
