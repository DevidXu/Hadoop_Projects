#!/bin/bash
RED='\033[0;31m'
NC='\033[0m'

printf "\n${RED}Start executing test.sh bash script!${NC}\n\n"

if [ ! -d classes ]; then 
	mkdir classes
fi
rm ./classes/*
printf "\n${RED}local old jar class has been removed!${NC}\n\n"

hadoop fs -rm /Tutorial/Output/*
hadoop fs -rmdir /Tutorial/Output
printf "\n${RED}Output directory under HDFS has been deleted!${NC}\n\n"

javac -classpath ${HADOOP_CLASSPATH} -d '/home/dewei/Hadoop/Workspace/JoinOrder/classes' -target 1.8 -source 1.8 JoinOrder.java
jar -cvf JoinOrder.jar -C classes/ .
printf "\n${RED}Jar package has been compiled! ${NC}\n\n"

hadoop jar JoinOrder.jar JoinOrder /Tutorial/Input /Tutorial/Output
hadoop fs -cat /Tutorial/Output/* > output
printf "\n${RED}Distributed computing has been completed! Please check local Output file!${NC}\n\n"
