package org.wso2.siddhi.debs2016.extensions;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.comment.TimeWindow;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;
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
public class RankerQuery1 extends StreamFunctionProcessor {
    private long startTimestamp;
    private long endTimestamp;
    private long count;
    private PostStore postStore;
    private CommentPostMap commentPostMap;
    private TimeWindow timeWindow;
    private long latency;
    private long numberOfOutputs;
    public static long timeOfEvent;
    public static boolean Q1_COMPLETED = false;
    private static final StringBuilder builder = new StringBuilder();
    long lastTimestamp;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext, boolean b) {
        if (expressionExecutors.length != 9) {

            System.err.println("Required Parameters : Six");
            return null;
        }
        postStore = new PostStore();
        commentPostMap = new CommentPostMap();
        timeWindow = new TimeWindow(postStore, commentPostMap);
        lastTimestamp = 0;

        ArrayList<Attribute> attributes = new ArrayList<Attribute>(13);
        attributes.add(new Attribute("result", Attribute.Type.STRING));

        return attributes;
    }

    @Override
    protected Object[] process(Object[] objects) {

        try {

            long timestamp = (Long) objects[0];
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


        } catch (Exception e) {
            e.printStackTrace();
        }

        return new Object[0];
    }

    private void flush(TimeWindow timeWindow, long ts) {
        ts = ts + CommentPostMap.DURATION / 24 / 60;
        boolean isEmpty = postStore.getPostScoreMap().isEmpty();
        boolean hasChanged;
        while (!isEmpty) {
            isEmpty = postStore.getPostScoreMap().isEmpty();
            hasChanged = timeWindow.updateTime(ts);
            if (hasChanged) {
                long endTime = postStore.printTopThreeComments(ts, false, true, ",");
                latency += (endTime - endTimestamp);
                numberOfOutputs++;
            }
            ts = ts + CommentPostMap.DURATION / 24 / 60;
        }
    }


    private void showFinalStatistics() {
        try {
            postStore.destroy();
            builder.setLength(0);
            long timeDifference = endTimestamp - startTimestamp;
            Date dNow = new Date();
            SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
            System.out.println("Query 1 completed .....at : " + dNow.getTime() + "--" + ft.format(dNow));
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
                builder.append(", ");
            } else {
                String latencyString = "000000";
                builder.append(latencyString);
            }
        } finally {
            Q1_COMPLETED = true;
            if (RankerQuery2.Q2_COMPLETED) {
                writeOutput();
                RankerQuery2.writeOutput();
                System.exit(0);
            }
        }
    }

    public static void writeOutput() {
        try {
            File performance = new File("performance.txt");
            BufferedWriter writer = new BufferedWriter(new FileWriter(performance, true));
            String result = builder.toString();
            writer.write(result);
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
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
