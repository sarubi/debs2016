package org.wso2.siddhi.debs2016.extensions;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.util.Constants;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by bhagya on 6/8/16.
 */
public class RankerQuery2 extends StreamFunctionProcessor {
    private static final StringBuilder builder = new StringBuilder();
    private static long startTimestamp;
    private static long endTimestamp;
    private Graph friendshipGraph;
    private CommentStore commentStore;
    private static int count;
    private static long latency;
    private static long numberOfOutputs;
    public static volatile boolean Q2_COMPLETED = false;
    int k;
    long duration;

    @Override
    protected Object[] process(Object[] objects) {
        try {
            long logicalTimestamp = (Long) objects[1];
            //Note that we cannot cast int to enum type. Java enums are classes. Hence we cannot cast them to int.
            int streamType = (Integer) objects[8];
            if (commentStore != null) {
                commentStore.cleanCommentStore(logicalTimestamp);
            }
            count++;

            switch (streamType) {
                case Constants.COMMENTS:
                    long commentId = (Long) objects[3];
                    String comment = (String) objects[4];
                    commentStore.registerComment(commentId, logicalTimestamp, comment);
                    break;
                case Constants.FRIENDSHIPS:
                    if (logicalTimestamp == -2) {
                        count--;
                        showFinalStatistics();
                        commentStore.destroy();
                        break;
                    } else if (logicalTimestamp == -1) {
                        count--;
                        startTimestamp = (long) objects[0];
                        duration = (long) objects[6];
                        k = (Integer) objects[7];
                        commentStore = new CommentStore(duration, friendshipGraph, k);
                        break;
                    } else {
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

            if (logicalTimestamp != -2 && logicalTimestamp != -1) {
                Long endTime = commentStore.computeKLargestComments(",", false, true);

                if (endTime != -1L) {
                    latency += (endTime - (long) objects[0]);
                    numberOfOutputs++;
                }

                endTimestamp = System.currentTimeMillis();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Object[0];
    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext, boolean b) {
        if (expressionExecutors.length != 9) {
            throw new RuntimeException("Required Parameters : Nine");
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        friendshipGraph = new Graph();
        commentStore = null;
        return attributeList;
    }

    private synchronized void showFinalStatistics() {
        try {
            commentStore.destroy();
            builder.setLength(0);
            long timeDifference = endTimestamp - startTimestamp;
            Date dNow = new Date();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
            System.out.println("Query 2 completed .....at : " + dNow.getTime() + "--" + simpleDateFormat.format(dNow));
            System.out.println("Event count : " + count);
            String timeDifferenceString = Float.toString(((float) timeDifference / 1000)) + "000000";
            System.out.println("Total run time : " + timeDifferenceString.substring(0, 7));
            builder.append(timeDifferenceString.substring(0, 7));
            builder.append(", ");

            System.out.println("Throughput (events/s): " + Math.round((count * 1000.0) / timeDifference));
            System.out.println("Total Latency " + latency);
            System.out.println("Total Outputs " + numberOfOutputs);
            if (numberOfOutputs != 0) {
                float temp = ((float) latency / numberOfOutputs) / 1000;
                BigDecimal averageLatency = new BigDecimal(temp);
                String latencyString = averageLatency.toPlainString() + "000000";
                System.out.println("Average Latency " + latencyString.substring(0, 7));
                builder.append(latencyString.substring(0, 7));
            } else {
                String latencyString = "000000";
                builder.append(latencyString);
            }
        } finally {
            RankerQuery2.Q2_COMPLETED = true;
            if (RankerQuery1.Q1_COMPLETED) {
                RankerQuery1.writeOutput();
                writeOutput();
                System.exit(0);
            }
        }
    }

    public static void writeOutput() {
        File performance = new File("performance.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(performance, true))) {
            String result = builder.toString();
            writer.write(result);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }
}
