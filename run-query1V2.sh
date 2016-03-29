#!/usr/bin/env bash
export M2_REPO_LOCATION=/home/bhagya/.m2
export CLASSPATH=$CLASSPATH:.:target/classes:lib/joda-time-2.9.2.jar:$CLASSPATH:$M2_REPO_LOCATION/repository/com/google/guava/guava/18.0/guava-18.0.jar:$M2_REPO_LOCATION/repository/org/antlr/antlr4-runtime/4.5.1/antlr4-runtime-4.5.1.jar:$M2_REPO_LOCATION/repository/org/wso2/siddhi/siddhi-query-api/3.0.6-SNAPSHOT/siddhi-query-api-3.0.6-SNAPSHOT.jar:$M2_REPO_LOCATION/repository/org/wso2/siddhi/siddhi-core/3.0.6-SNAPSHOT/siddhi-core-3.0.6-SNAPSHOT.jar:$M2_REPO_LOCATION/repository/org/wso2/siddhi/siddhi-query-compiler/3.0.6-SNAPSHOT/siddhi-query-compiler-3.0.6-SNAPSHOT.jar:$M2_REPO_LOCATION/repository/org/wso2/orbit/com/lmax/disruptor/3.3.2.wso2v2/disruptor-3.3.2.wso2v2.jar:$M2_REPO_LOCATION/repository/org/apache/log4j/wso2/log4j/1.2.17.wso2v1/log4j-1.2.17.wso2v1.jar

java -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Xmx8g -Xms8g -cp $CLASSPATH org.wso2.siddhi.debs2016.query.Query1V2 $1 $2
