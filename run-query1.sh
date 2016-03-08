#!/usr/bin/env bash
export M2_REPO_LOCATION=/home/miyurud/.m2
export CLASSPATH=$CLASSPATH:.:target/classes:lib/joda-time-2.9.2.jar:$CLASSPATH:$M2_REPO_LOCATION/repository/com/google/guava/guava/13.0.1/guava-13.0.1.jar:$M2_REPO_LOCATION/repository/org/antlr/antlr-runtime/3.4/antlr-runtime-3.4.jar:$M2_REPO_LOCATION/repository/org/wso2/siddhi/siddhi-query-api/3.2.1.wso2v1/siddhi-query-api-3.2.1.wso2v1.jar:$M2_REPO_LOCATION/repository/org/wso2/siddhi/siddhi-core/3.2.1.wso2v1/siddhi-core-3.2.1.wso2v1.jar:$M2_REPO_LOCATION/repository/org/wso2/siddhi/siddhi-query-compiler/3.2.1.wso2v1/siddhi-query-compiler-3.2.1.wso2v1.jar:$M2_REPO_LOCATION/repository/com/googlecode/disruptor/disruptor/3.2.1.wso2v1/target/disruptor-3.2.1.wso2v1.jar
export DATASETS_FOLDER=/home/miyurud/DEBS2016/DataSet/data

java -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Xmx8g -Xms8g -cp $CLASSPATH org.wso2.siddhi.debs2016.query.Query1 $DATASETS_FOLDER
