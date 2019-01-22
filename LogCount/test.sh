#!/bin/bash
RED='\033[0;31m'
NC='\033[0m'

printf "\n${RED}Start executing test.sh bash script!${NC}\n\n"

hadoop fs -rm /Tutorial/Output/*
hadoop fs -rmdir /Tutorial/Output
printf "\n${RED}Output directory on HDFS has been deleted!${NC}\n\n"

rm ./classes/*
javac -classpath ${HADOOP_CLASSPATH} -d '/home/dewei/Hadoop/LogCount/classes' -target 1.8 -source 1.8 LogCount.java
jar -cvf LogCount.jar -C classes/ .
printf "\n${RED}Jar package has been compiled! ${NC}\n\n"

hadoop jar LogCount.jar LogCount /Tutorial/Input /Tutorial/Output
hadoop fs -cat /Tutorial/Output/* > output
printf "\n${RED}Distributed computing has been completed! Please check local Output file!${NC}\n\n"
