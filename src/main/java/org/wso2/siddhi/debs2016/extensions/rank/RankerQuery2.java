package org.wso2.siddhi.debs2016.extensions.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.util.Constants;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
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
    private long duration=10;
    public static Graph FRIENDSHIPGRAPH = new Graph();
    private CommentStore commentStore = new CommentStore(duration);
    private int k = 10;

    /**
     * Process the merge stream
     */
    @Override
    protected Object[] process(Object[] objects) {
        long iij_timestamp = (Long)objects[0];
        long ts = (Long)objects[1];
        long user_id_1 = (Long) objects [2]; //Note that user_id_1 is common for both friendship_user_id_1 and like_user_id
        //Note that we cannot cast int to enum type. Java enums are classes. Hence we cannot cast them to int.
        int streamType= (Integer) objects[8];

        commentStore.updateCommentStore(ts);

        switch(streamType) {
            case Constants.COMMENTS:
                long comment_id = (Long)objects[3];
                String comment = (String)objects[4];
                //System.out.println("Comment");
                commentStore.registerComment(comment_id, ts,comment);
                break;
            case Constants.FRIENDSHIPS:
                long friendship_user_id_2 = (Long) objects [3];
                //System.out.println("Friendship");
                FRIENDSHIPGRAPH.addEdge(user_id_1, friendship_user_id_2);
                commentStore.handleNewFriendship(user_id_1,friendship_user_id_2);
                break;
            case Constants.LIKES:
                long like_comment_id = (Long) objects [3];
               // System.out.println("Like");
                commentStore.registerLike(like_comment_id, user_id_1);
                break;
        }
        String comments [] = commentStore.getKLargestComments(k);
        System.out.println("numOfComments in Store = " + commentStore.getNumberOfComments() + "," + ts + " : ");

        for(int i = 0; i < comments.length;i++) {

            System.out.println(commentStore.getKLargestComments(k) + ",");
        }

        return objects;
    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
    }


    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 9) {
            throw new RuntimeException("Required Parameters : Nine");
        }
        List<Attribute> attributeList = new ArrayList<Attribute>();
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
