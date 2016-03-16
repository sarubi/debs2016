package org.wso2.siddhi.debs2016.extensions.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.util.Constants;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *
 * The ranker implementation for query for 2
 *
 */
public class RankerQuery2 extends StreamFunctionProcessor {
    private Graph friendsGraph;
    private String iij_timestamp;
    private String ts;
    private long duration= 10000000l;
    public  Graph friendshipGraph ;
    private CommentStore commentStore ;
    private int k = 20;
    private static int count = 1;
    long timeDifferenceFromStart = 0;
    long timeDifference = 0; //This is the time difference for this time window.
    long currentTime = 0;
    long prevTime = 0;
    long startTime = 0;
    long cTime = 0;
    private Date startDateTime;
    /**
     * Process the merge stream
     */
    @Override
    protected Object[] process(Object[] objects) {
        try{

        long iij_timestamp = (Long) objects[0];
        long ts = (Long) objects[1];
        long user_id_1 = (Long) objects[2]; //Note that user_id_1 is common for both friendship_user_id_1 and like_user_id
        //Note that we cannot cast int to enum type. Java enums are classes. Hence we cannot cast them to int.
        int streamType = (Integer) objects[8];
            commentStore.updateCommentStore(ts);

            count++;

//                System.out.println(ts);

        switch (streamType) {
            case Constants.COMMENTS:
                long comment_id = (Long) objects[3];
                String comment = (String) objects[4];
                commentStore.registerComment(comment_id, ts, comment, false);
                break;
            case Constants.FRIENDSHIPS:
                long friendship_user_id_2 = (Long) objects[3];
                //System.out.println("Friendship");
                friendshipGraph.addEdge(user_id_1, friendship_user_id_2);
                commentStore.handleNewFriendship(user_id_1, friendship_user_id_2);
                break;
            case Constants.LIKES:
                long like_comment_id = (Long) objects[3];
                // System.out.println("Like");
                commentStore.registerLike(like_comment_id, user_id_1);
                break;
        }
            commentStore.computeKLargestComments(k, " : " , false);

            showStatistics(1231511);

    }catch (Exception e)
    {
        e.printStackTrace();
    }

        return objects;
    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
    }

    /**
     *
     * Print the throughput etc
     *
     * @param maxNumEvents max number of events
     */
    private void showStatistics(int maxNumEvents)
    {

        if(count >  maxNumEvents) {

            currentTime = System.currentTimeMillis();
            timeDifferenceFromStart = (currentTime - startTime);
            timeDifference = currentTime - prevTime;

            Date dNow = new Date();
            SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
            System.out.println("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
            System.out.println("Event count : " + count);
            timeDifferenceFromStart = dNow.getTime() - startDateTime.getTime();
            System.out.println("Total run time : " + timeDifferenceFromStart);
            System.out.println("Average input data rate (events/s): " + Math.round((count * 1000.0) / timeDifferenceFromStart));
            System.out.flush();
        }
    }


    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 9) {
            throw new RuntimeException("Required Parameters : Nine");
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
        friendshipGraph = new Graph();
        commentStore = new CommentStore(duration, friendshipGraph);

            //We print the start and the end times of the experiment even if the performance logging is disabled.
        startDateTime = new Date();
        startTime = startDateTime.getTime();
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
        System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));


        return attributeList;
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
}
