package org.wso2.siddhi.debs2016.extensions.rank;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.input.FileType;
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
    public static Graph FRIENDSHIPGRAPH = new Graph();
    private CommentStore commentStore = new CommentStore();
    private int k = 10;

    /**
     * Process the merge stream
     */
    @Override
    protected Object[] process(Object[] objects) {

        long iij_timestamp = (Long)objects[0];
        long ts = (Long)objects[1];
        long comment_id = (Long)objects[2];
        String comment = (String)objects[3];
        long comment_user_id = (Long) objects[4];
        long friendship_user_id_1 = (Long) objects [5];
        long friendship_user_id_2 = (Long) objects [6];
        long like_user_id = (Long) objects [7];
        long like_comment_id = (Long) objects [8];
        FileType streamType= (FileType) objects[9];

        commentStore.updateCommentStore(ts);

        switch(streamType) {
            case COMMENTS:
                commentStore.registerComment(comment_id, ts,comment);
                break;
            case FRIENDSHIPS:
                FRIENDSHIPGRAPH.addEdge(friendship_user_id_1, friendship_user_id_2);
                commentStore.handleNewFriendship(friendship_user_id_1,friendship_user_id_2);
                break;
            case LIKES:
                commentStore.registerLike(like_comment_id, like_user_id);
                break;
        }
        commentStore.getKLargestComments(k);

        return objects;
    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
    }


    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 10) {
            throw new RuntimeException("Required Parameters : Six");
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
