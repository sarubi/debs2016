/*
 *  Copyright (c) 2016 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.siddhi.debs2016.Processors;

import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ2;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Q2EventSingle implements Runnable {

    private static final StringBuilder builder = new StringBuilder();
    private static long startTimestamp;
    private static long endTimestamp;
    private final Graph friendshipGraph ;
    private final CommentStore commentStore;
    private static int count;
    private static long latency;
    private static long  numberOfOutputs;
    static volatile boolean Q2_COMPLETED = false;

    private OrderedEventSenderThreadQ2 orderedEventSenderThreadQ2;

    public Q2EventSingle(OrderedEventSenderThreadQ2 orderedEventSenderThreadQ2, long d, int k){
        this.orderedEventSenderThreadQ2 = orderedEventSenderThreadQ2;
        friendshipGraph = new Graph();
        commentStore = new CommentStore(d, friendshipGraph, k);
    }


    private static boolean firstEvent = true;

    @Override
    public void run() {

        while(true){
            boolean finished = false;

            try{
                Object[] objects;
                if (!firstEvent){
                    objects = this.orderedEventSenderThreadQ2.getNextEvent();
                } else {
                    objects = new Object[]{
                            0L,
                            -1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            0L,
                            0L,
                            0,
                    };
                    firstEvent = false;
                }

                long timestamp = System.currentTimeMillis();

                long logicalTimestamp = (Long) objects[1];
                //Note that we cannot cast int to enum type. Java enums are classes. Hence we cannot cast them to int.
                int streamType = (Integer) objects[8];
                commentStore.cleanCommentStore(logicalTimestamp);
                count++;

                switch (streamType) {
                    case Constants.COMMENTS:
                        long commentId = (Long) objects[3];
                        String comment = (String) objects[4];
                        commentStore.registerComment(commentId, logicalTimestamp, comment);
                        break;
                    case Constants.FRIENDSHIPS:
                        if (logicalTimestamp == -2){
                            count--;
                            showFinalStatistics();
                            commentStore.destroy();
                            finished = true;
                            break;
                        }else if (logicalTimestamp == -1) {
                            count--;
//                            startTimestamp = debsEvent.getSystemArrivalTime();
                            startTimestamp = timestamp;
                            break;
                        }else{
                            long userOneId = (Long) objects[2];
                            long userTwoId = (Long) objects[3];
                            friendshipGraph.addEdge(userOneId, userTwoId);
                            commentStore.handleNewFriendship(userOneId, userTwoId);
                            break;
                        }
                    case Constants.LIKES:
                        long userId = (Long) objects[2];
                        long likeCommentId = (Long) objects[3];
                        commentStore.registerLike(likeCommentId, userId);
                        break;
                }

                if (logicalTimestamp != -2 && logicalTimestamp != -1){
                    Long endTime = commentStore.computeKLargestComments("," ,false, true);

                    if (endTime != -1L){
                        latency += (endTime - timestamp);
                        numberOfOutputs++;
                    }

                    endTimestamp = System.currentTimeMillis();
                }

                if (finished){
                    break;
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }

    }

    private synchronized void showFinalStatistics()
    {
        try {
            commentStore.destroy();
            builder.setLength(0);
            long timeDifference = endTimestamp - startTimestamp;
            Date dNow = new Date();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
            System.out.println("Query 2 completed .....at : " + dNow.getTime() + "--" + simpleDateFormat.format(dNow));
            System.out.println("Event count : " + count);
            String timeDifferenceString = Float.toString(((float) timeDifference /1000)) + "000000";
            System.out.println("Total run time : " + timeDifferenceString.substring(0, 7));
            builder.append(timeDifferenceString.substring(0, 7));
            builder.append(", ");

            System.out.println("Throughput (events/s): " + Math.round((count * 1000.0) / timeDifference));
            System.out.println("Total Latency " + latency);
            System.out.println("Total Outputs " + numberOfOutputs);
            if (numberOfOutputs != 0) {
                float temp = ((float)latency/numberOfOutputs)/1000;
                BigDecimal averageLatency = new BigDecimal(temp);
                String latencyString = averageLatency.toPlainString() + "000000";
                System.out.println("Average Latency " + latencyString.substring(0, 7));
                builder.append(latencyString.substring(0, 7));
            } else {
                String latencyString = "000000";
                builder.append(latencyString);
            }
        }finally {
            Q2_COMPLETED = true;
            if(Q1EventSingle.Q1_COMPLETED)
            {
                Q1EventSingle.writeOutput();
                writeOutput();
                System.exit(0);
            }
        }
    }

    /**
     * Writes the output to the file
     */
    public static void writeOutput() {
        File performance = new File("performance.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(performance, true))) {
            String result = builder.toString();
            writer.write(result);
            writer.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
