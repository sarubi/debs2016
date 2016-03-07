package org.wso2.siddhi.debs2016.extensions.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.*;


public class Ranker extends StreamFunctionProcessor {
    private HashMap<Long, TreeSet<Long>> commentsForPostMap; //Key is a post_id, values is the list of comments posted to that post
    private HashMap<Long, TreeSet<Long>> commentsForCommentMap; //Key is a comment_id, values is the list of comments posted to that comment
    private TreeMap<Long, TreeSet<Long>> postRankTreeMap; //Key is the total score of the post, value is the list of posts which has that particular total score
    private TreeMap<Long, TreeSet<Long>> commentRankTreeMap; //Key is the total score of the comment, value is the list of comments which has that particular total score
    private TreeMap<Long, TreeSet<Long>> commentLifeTimeTreeMap;
    private TreeMap<Long, TreeSet<Long>> postLifeTimeTreeMap;
    private String iij_timestamp;
    private String ts;
    private String postID;
    private String commentID;
    private String commentRepliedID;
    private String isPostFlag;

    @Override
    protected Object[] process(Object[] objects) {
        //iij_timestamp, ts, post_id, -1 as comment_id, -1 as comment_replied_id
        long iij_timestamp = (Long)objects[0];
        long ts = (Long)objects[1];
        long post_id = (Long)objects[2];
        long comment_id = (Long)objects[3];
        long comment_replied_id = (Long)objects[4];
        boolean isPostFlag = (Boolean)objects[5];

        //For each incoming post or comment we have to add them to the appropriate data structure with their initial scores
        if(isPostFlag){ //This is a new post
            //By default we know that all the post ids are new ones
            TreeSet<Long> treeSet = new TreeSet<Long>(); //Since this is post only, initial list of comments will be zero.
            commentsForPostMap.put(post_id, treeSet);
            treeSet = postRankTreeMap.get(10);
            treeSet.add(post_id);//Initially the total score of the new post is 10.

            TreeSet<Long> posts = postLifeTimeTreeMap.get(ts);
            if(posts==null){
                posts = new TreeSet<Long>();
            }

            posts.add(post_id);
            postLifeTimeTreeMap.put(ts, posts);
        } else {
            if(comment_replied_id == -1){ //This is a comment posted to an existing post
                TreeSet<Long> treeSet = commentsForPostMap.get(post_id);
                treeSet.add(comment_id);
                treeSet = commentRankTreeMap.get(10);
                treeSet.add(comment_id);//Initially the total score of the new comment is 10.
            } else if(comment_replied_id != -1) { //This is a comment posted to an existing comment
                TreeSet<Long> treeSet = commentsForCommentMap.get(comment_replied_id);
                treeSet.add(comment_id);
                treeSet = commentRankTreeMap.get(10);
                treeSet.add(comment_id);//Initially the total score of the new comment is 10.
            }
        }



        return objects;
    }

    @Override
    protected Object[] process(Object o) {
        return new Object[0];
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 6) {
            System.err.println("Required Parameters : Six");
            return null;
        }

        postRankTreeMap = new TreeMap<Long, TreeSet<Long>>();
        commentsForPostMap = new HashMap<Long, TreeSet<Long>>();

        //By default we know that all the posts entered to the score map has a default score of 10
        TreeSet<Long> treeSet = new TreeSet<Long>();
        postRankTreeMap.put(10l, treeSet);

        iij_timestamp =((VariableExpressionExecutor) expressionExecutors[0]).getAttribute().getName();
        ts =((VariableExpressionExecutor) expressionExecutors[1]).getAttribute().getName();
        postID =((VariableExpressionExecutor) expressionExecutors[2]).getAttribute().getName();
        commentID =((VariableExpressionExecutor) expressionExecutors[3]).getAttribute().getName();
        commentRepliedID = ((VariableExpressionExecutor) expressionExecutors[4]).getAttribute().getName();
        isPostFlag = ((VariableExpressionExecutor) expressionExecutors[5]).getAttribute().getName();

        List<Attribute> attributeList = new ArrayList<Attribute>();

        attributeList.add(new Attribute("iij_timestamp", Attribute.Type.LONG));
        attributeList.add(new Attribute("ts", Attribute.Type.LONG));
        attributeList.add(new Attribute("postID", Attribute.Type.LONG));
        attributeList.add(new Attribute("commentID", Attribute.Type.LONG));
        attributeList.add(new Attribute("commentRepliedID", Attribute.Type.LONG));
        attributeList.add(new Attribute("isPostFlag", Attribute.Type.BOOL));

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
