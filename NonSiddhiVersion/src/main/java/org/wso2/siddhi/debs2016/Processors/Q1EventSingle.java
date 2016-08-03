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

import org.wso2.siddhi.debs2016.comment.TimeWindow;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ1;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Q1EventSingle implements Runnable{

    private long startTimestamp;
    private long endTimestamp;
    private long count;
    private final PostStore postStore;
    private final CommentPostMap commentPostMap;
    private final TimeWindow timeWindow;
    private long latency;
    private long numberOfOutputs;
    public static long timeOfEvent;
    static boolean Q1_COMPLETED  = false;
    private static final StringBuilder builder = new StringBuilder();

    private OrderedEventSenderThreadQ1 orderedEventSenderThreadQ1;

    /**
     * Constructor
     */
    public Q1EventSingle(OrderedEventSenderThreadQ1 orderedEventSenderThreadQ1){
        this.orderedEventSenderThreadQ1 = orderedEventSenderThreadQ1;

        postStore = new PostStore();
        commentPostMap = new CommentPostMap();
        timeWindow = new TimeWindow(postStore, commentPostMap);
    }


    private long lastTimestamp = 0;
     private static boolean firstEvent = true;
    /**
     * Main Method
     */
    public void run() {

        while (true) {
            Object[] objects;
            boolean finished = false;
            if (!firstEvent){
                objects = orderedEventSenderThreadQ1.getNextEvent();
            } else {
                firstEvent = false;
                objects = new Object[]{
                        0L,
                        -1L,
                        0L,
                        0L,
                        "",
                        "",
                        0L,
                        0L,
                        Constants.POSTS
                };
            }

            try {

                long timestamp = System.currentTimeMillis(); //TODO: Take System Current Time Here

                endTimestamp = timestamp;

                long logicalTimestamp = (Long) objects[1];
                String userName = (String) objects[5];
                int isPostFlag = (int) objects[8];

                count++;
                Post post;
                boolean hasTopChanged;
                switch (isPostFlag) {
                    case Constants.POSTS:
                        if (logicalTimestamp == -1L) {
                            //This is the place where time measuring starts.
                            count--;
                            startTimestamp = timestamp;
                            break;
                        }

                        if (logicalTimestamp == -2L) {
                            //This is the place where time measuring ends.
                            count--;
                            flush(timeWindow, lastTimestamp);
                            showFinalStatistics();
                            postStore.destroy();
                            finished = true;
                            break;
                        }
                        long post_id = (Long) objects[2];
                        post = postStore.addPost(post_id, logicalTimestamp, userName); // 2)
                        timeWindow.updateTime(logicalTimestamp);
                        timeWindow.addNewPost(logicalTimestamp, post);
                        if (postStore.hasTopThreeChanged()) {
                            long endTime = postStore.printTopThreeComments(logicalTimestamp, false, true, ",");
                            latency += (endTime - timestamp);
                            numberOfOutputs++;
                        }
                        break;

                    case Constants.COMMENTS:
                        long commentId = (Long) objects[3];
                        long commentRepliedId = (Long) objects[6];
                        long postRepliedId = (Long) objects[7];
                        long commenterId = (Long) objects[2];

                        if (postRepliedId != -1 && commentRepliedId == -1) {
                            boolean hasUpdateChangedTop = timeWindow.updateTime(logicalTimestamp);
                            post = postStore.getPost(postRepliedId);
                            if (post != null) {
                                timeWindow.addComment(post, logicalTimestamp, commenterId);
                            }
                            hasTopChanged = postStore.hasTopThreeChanged();
                            if (hasTopChanged || hasUpdateChangedTop) {
                                if (postStore.getPost(postRepliedId) == null) {
                                    long endTime = postStore.printTopThreeComments(timeOfEvent, false, true, ",");
                                    latency += (endTime - timestamp);
                                    numberOfOutputs++;
                                } else {
                                    long endTime = postStore.printTopThreeComments(logicalTimestamp, false, true, ",");
                                    latency += (endTime - timestamp);
                                    numberOfOutputs++;
                                }
                            }
                            commentPostMap.addCommentToPost(commentId, postRepliedId);

                        } else if (commentRepliedId != -1 && postRepliedId == -1) {
                            long parent_post_id = commentPostMap.addCommentToComment(commentId, commentRepliedId);
                            boolean hasUpdateChangedTop = timeWindow.updateTime(logicalTimestamp);
                            post = postStore.getPost(parent_post_id);
                            if (post != null) {
                                timeWindow.addComment(post, logicalTimestamp, commenterId);
                            }
                            hasTopChanged = postStore.hasTopThreeChanged();
                            if (hasUpdateChangedTop || hasTopChanged) {
                                if (postStore.getPost(postRepliedId) == null) {
                                    long endTime = postStore.printTopThreeComments(timeOfEvent, false, true, ",");
                                    latency += (endTime - timestamp);
                                    numberOfOutputs++;
                                } else {
                                    long endTime = postStore.printTopThreeComments(logicalTimestamp, false, true, ",");
                                    latency += (endTime - timestamp);
                                    numberOfOutputs++;
                                }
                            }
                        }
                        break;
                }

                lastTimestamp = logicalTimestamp;

                if (finished){
                    break;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Flush each remaining tuple every 24 hours until the post store ie empty
     *
     * @param timeWindow the time window
     * @param ts the arrival time of the last event
     */
    private void flush(TimeWindow timeWindow, long ts)
    {
        ts = ts +  CommentPostMap.DURATION/24/60;
        boolean isEmpty = postStore.getPostScoreMap().isEmpty();
        boolean hasChanged;
        while(!isEmpty) {
            isEmpty = postStore.getPostScoreMap().isEmpty();
            hasChanged = timeWindow.updateTime(ts);
            if (hasChanged){
                long endTime =  postStore.printTopThreeComments(ts, false, true, ",");
                latency += (endTime - endTimestamp);
                numberOfOutputs++;
            }
            ts = ts +  CommentPostMap.DURATION/24/60;
        }
    }

    /**
     * Writes the output to the file
     */
    public static void writeOutput() {
        try {
            File performance = new File("performance.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(performance, true));
            String result = builder.toString();
            writer.write(result);
            writer.close();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
    /**
     *
     * Print the throughput etc
     *
     */
    private void showFinalStatistics()
    {
        try{
            postStore.destroy();
            builder.setLength(0);
            long timeDifference = endTimestamp - startTimestamp;
            Date dNow = new Date();
            SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
            System.out.println("Query 1 completed .....at : " + dNow.getTime() + "--" + ft.format(dNow));
            System.out.println("Event count : " + count);
            String timeDifferenceString = Float.toString(((float) timeDifference /1000)) + "000000";
            System.out.println("Total run time : " + timeDifferenceString.substring(0, 7));
            builder.append(timeDifferenceString.substring(0, 7));
            builder.append(", ");
            System.out.println("Throughput (events/s): " + Math.round((count * 1000.0) / timeDifference));
            System.out.println("Total Latency " + latency);
            System.out.println("Total Outputs " + numberOfOutputs);
            if (numberOfOutputs!=0){
                float temp = ((float)latency/numberOfOutputs)/1000;
                BigDecimal averageLatency = new BigDecimal(temp);
                String latencyString = averageLatency.toPlainString() + "000000";
                System.out.println("Average Latency " + latencyString.substring(0, 7));
                builder.append(latencyString.substring(0, 7));
                builder.append(", ");
            } else {
                String latencyString = "000000";
                builder.append(latencyString);
            }
        }finally {
            Q1_COMPLETED = true;
            if(Q2EventSingle.Q2_COMPLETED){
                writeOutput();
                Q2EventSingle.writeOutput();
                System.exit(0);
            }
        }
    }
}
