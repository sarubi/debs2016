package org.wso2.siddhi.debs2016.extensions.rank;

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

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by anoukh on 3/25/16.
 */
public class RankerQuery1V2 extends StreamFunctionProcessor {

    private long startiij_timestamp;
    private long endiij_timestamp;
    private long count;
    private Date startDateTime;
    long startTime = 0;

    private PostStore postStore;
    private CommentPostMap commentPostMap;
    private TimeWindow timeWindow;

    private Long latency = 0L;
    private Long numberOfOutputs = 0L;

    @Override
    protected Object[] process(Object[] objects) {
        try {
            long iij_timestamp = (Long) objects[0];
            endiij_timestamp = iij_timestamp;
            long ts = (Long) objects[1];
            String user_name = (String) objects[5];
            int isPostFlag = (int) objects[8];

            if (ts == -1L) {
                //This is the place where time measuring starts.
                startiij_timestamp = iij_timestamp;
                return new Object[]{""};
            }

            if (ts == -2L) {
                //This is the place where time measuring ends.
                showFinalStatistics();
                postStore.destroy();
                return new Object[]{""};

            }
            count++;

            /*Answer: When processing a new input tuple, processing steps should be performed in this order:
             1) decrease the score of all previous posts (given the semantics of the query)
             2) increase score of post related to the input tuple,
             3) decrease score of posts expiring precisely on this timestamp, if any,
             4) discard posts with 0 score. Such a post would, thus, survive the transient state. However, a post whose score reached 0 at a timestamp earlier than the current input tuple, will not survive, even if the processing of that timeout happens to be triggered by the current input tuple.
             */

            // Does it make difference in the final result whether we perform 1) 2) 3) in order this order or not?
            // What is the plan for commentPostMap?
            // What aren't we doing timeWindow(ts) at the start?

            //The order given necessarily means that we have to delete a post after updating the store
            //We need commentPostMap to point a comment to comment to the correct post (Eliminates recursion)

            Post post;
            switch (isPostFlag){
                case Constants.POSTS:
                    long post_id = (Long) objects[2];
                    post = postStore.addPost(post_id, ts, user_name); // 2)
                    timeWindow.updateTime(ts);
                    timeWindow.addNewPost(ts, post);
                    break;

                case Constants.COMMENTS:
                    long comment_id = (Long) objects[3];
                    long comment_replied_id = (Long) objects[6];
                    long post_replied_id = (Long) objects[7];
                    long commenter_id = (Long) objects[2];

                    if (post_replied_id != -1 && comment_replied_id == -1){
                        post = postStore.getPost(post_replied_id);
                        if (post != null) {
                            timeWindow.addComment(post, ts, commenter_id);
                        }
                        timeWindow.updateTime(ts);
                        commentPostMap.addCommentToPost(comment_id, post_replied_id);

                    } else if (comment_replied_id != -1 && post_replied_id == -1){
                        long parent_post_id = commentPostMap.addCommentToComment(comment_id, comment_replied_id);
                        post = postStore.getPost(parent_post_id);
                        if (post != null) {
                            timeWindow.addComment(post, ts, commenter_id);
                        }
                        timeWindow.updateTime(ts);
                    }
                    break;
            }
//            Long endTime= -1L;
            Long endTime = postStore.printTopThreeComments(ts, false, true, ",");
            if (endTime != -1L){
                latency += (endTime - (Long) objects[0]);
                numberOfOutputs++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return objects;
    }

    @Override
    protected Object[] process(Object o) {
        return null;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (expressionExecutors.length != 9) {
            System.err.println("Required Parameters : Nine");
            return null;
        }

        postStore = new PostStore();
        commentPostMap = new CommentPostMap();
        timeWindow = new TimeWindow(postStore);

        startDateTime = new Date();
        startTime = startDateTime.getTime();
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");

        System.out.println("Ranker Query 1 V2");
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(13);
        attributes.add(new Attribute("result", Attribute.Type.STRING));

        return attributes;
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[0];
    }

    public void restoreState(Object[] objects) {

    }

    private void showFinalStatistics()
    {
        long timeDifference = endiij_timestamp - startiij_timestamp;

        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
        System.out.println("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
        System.out.println("Event count : " + count);
        System.out.println("Total run time : " + timeDifference);
        System.out.println("Throughput (events/s): " + Math.round((count * 1000.0) / timeDifference));
        System.out.println("Total Latency " + latency);
        System.out.println("Total Outputs " + numberOfOutputs);
        if(numberOfOutputs!=0){
            System.out.println("Average Latency " + latency/numberOfOutputs);
        }

        System.out.flush();
    }
}


