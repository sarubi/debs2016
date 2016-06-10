#!/usr/bin/env bash
java -XX:+UnlockCommercialFeatures -Xmx4g -Xms4g -cp target/debs2016gc-0.10-jar-with-dependencies.jar org.wso2.siddhi.debs2016.test.QueryTest 2

files=(
    "./data/2/Q1.2.txt"
    "./q1.txt"
    "./data/2/Q2.2.txt"
    "./q2.txt"
)



cmp -s ${files[0]} ${files[1]} > /dev/null
if [ $? -eq 1 ]; then
    echo Q1 result is different from expected results
else
    echo Q1 result is same as expected results
fi

cmp -s ${files[2]} ${files[3]} > /dev/null
if [ $? -eq 1 ]; then
    echo Q2 result is different from expected results
else
    echo Q2 result is same as expected results
fi