package org.wso2.siddhi.debs2016.extensions.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.input.CommentRecord;
import org.wso2.siddhi.debs2016.input.PostRecord;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.*;


public class RankerQuery1 extends StreamFunctionProcessor {
    private HashMap<Long, TreeSet<Long>> commentsForPostMap; //Key is a post_id, values is the list of comments posted to that post
    private HashMap<Long, TreeSet<Long>> commentsForCommentMap; //Key is a comment_id, values is the list of comments posted to that comment
    private TreeMap<Long, TreeSet<Long>> postRankTreeMap; //Key is the total score of the post, value is the list of posts which has that particular total score
    private TreeMap<Long, TreeSet<Long>> commentRankTreeMap; //Key is the total score of the comment, value is the list of comments which has that particular total score
    //private LinkedList<PostRecord> posts;
    //private LinkedList<CommentRecord> comments;

    private HashMap<Long, PostRecord> posts;
    private HashMap<Long, CommentRecord> comments;

    private String iij_timestamp;
    private String ts;
    private String postID;
    private String commentID;
    private String commentRepliedID;
    private String isPostFlag;
    private long[] topThreePosts;

    private long MILISECONDS_FOR_DAY = 24 * 3600 * 1000;

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
            treeSet = postRankTreeMap.get(10l);
            treeSet.add(post_id);//Initially the total score of the new post is 10.

            posts.put(post_id, new PostRecord(post_id, ts, 10));
        } else {
            TreeSet<Long> treeSet = null;

            if(comment_replied_id == -1){ //This is a comment posted to an existing post
                treeSet = commentsForPostMap.get(post_id);
                treeSet.add(comment_id);
            } else if(comment_replied_id != -1) { //This is a comment posted to an existing comment
                treeSet = commentsForCommentMap.get(comment_replied_id);
                treeSet.add(comment_id);
            }
            treeSet = commentRankTreeMap.get(10l);
            treeSet.add(comment_id);//Initially the total score of the new comment is 10.
            comments.put(comment_id, new CommentRecord(comment_id, ts, 10));
        }

        //Next, based on the timestamp value of this event object, we have to reduce the scores of the posts and the comments which are already stored in the
        //two linked lists
        Iterator<Map.Entry<Long, PostRecord>> postsIterator = posts.entrySet().iterator();
        PostRecord pst = null;

        while(postsIterator.hasNext()){
            pst = postsIterator.next().getValue();
            int scoreReduction = (int)((ts - pst.ts)/MILISECONDS_FOR_DAY);
            if(scoreReduction < 10){
                pst.score = (10 - scoreReduction);
            }
        }

        Iterator<Map.Entry<Long, CommentRecord>> commentsIterator = comments.entrySet().iterator();
        CommentRecord comment = null;

        while(commentsIterator.hasNext()){
            comment = commentsIterator.next().getValue();
            int scoreReduction = (int)((ts - comment.ts)/MILISECONDS_FOR_DAY);
            if(scoreReduction < 10){
                comment.score = (10 - scoreReduction);
            }
        }

        //Next, we need to calculate total score of each post and then decide whether we should discard posts.
        //We need to do this for each and every post
        postsIterator = posts.entrySet().iterator();

        while(postsIterator.hasNext()) {
            pst = postsIterator.next().getValue();
            long totalScore = pst.score;
            TreeSet<Long> commentsForPost = commentsForPostMap.get(pst.post_id);
            Iterator<Long> itr = commentsForPost.iterator();

            while(itr.hasNext()){
                long commentID = itr.next();
                Iterator<Long> itr2 = commentsForCommentMap.get(commentID).iterator();
                while(itr2.hasNext()){
                    long commentForCommetID = itr2.next();
                    totalScore += comments.get(commentForCommetID).score;
                }
                totalScore += comments.get(commentID).score;
            }

            if(pst.score != totalScore) {
                //First, we need to remove the Post object from the existing list
                TreeSet<Long> treeSet = postRankTreeMap.get(pst.score);
                treeSet.remove(pst);

                //Next, we need to add the new score along with the post to the tree
                treeSet = postRankTreeMap.get(totalScore);
                treeSet.add(totalScore);
                postRankTreeMap.put(totalScore, treeSet);
            }
        }

        if(updateRanks()){
//            <ts,top1_post_id,top1_post_user,top1_post_score,top1_post_commenters,
//                    top2_post_id,top2_post_user,top2_post_score,top2_post_commenters,
//                    top3_post_id,top3_post_user,top3_post_score,top3_post_commenters>
            Object[] result = new Object[13];//There are thirteen fields in this record
            result[0] = ts;
            Iterator<Map.Entry<Long, TreeSet<Long>>> itr = postRankTreeMap.entrySet().iterator();
            int counter = 0;
            while(itr.hasNext()){
                Map.Entry<Long, TreeSet<Long>> item = itr.next();
                Iterator<Long> itr2 = item.getValue().iterator();

                while(itr2.hasNext()) {
                    long postID = itr2.next();
                    result[1 + (counter * 4)] = postID;
                    result[2 + (counter * 4)] = posts.get(postID);//this should be topn_post_user

                }
            }
        }

        return null;
    }

    private boolean updateRanks(){
        boolean result = false;
        Iterator<Map.Entry<Long, TreeSet<Long>>> itr = postRankTreeMap.entrySet().iterator();
        int counter = 0;
        while(itr.hasNext()){
            Map.Entry item = itr.next();
            if(topThreePosts[counter] != item.getValue()){
                result = true;
            }

            counter++;

            if(counter > 2){//We need only the top three posts from the postRankTreeMap
                break;
            }
        }
        return result;
    }

    @Override
    protected Object[] process(Object o) {
        return null;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 6) {
            System.err.println("Required Parameters : Six");
            return null;
        }

        topThreePosts = new long[3];
        postRankTreeMap = new TreeMap<Long, TreeSet<Long>>();
        commentsForPostMap = new HashMap<Long, TreeSet<Long>>();
        commentsForCommentMap = new HashMap<Long, TreeSet<Long>>();
        commentRankTreeMap = new TreeMap<Long, TreeSet<Long>>();
        posts = new HashMap<Long, PostRecord>();
        comments = new HashMap<Long, CommentRecord>();

        //By default we know that all the posts entered to the score map has a default score of 10
        TreeSet<Long> treeSet = new TreeSet<Long>();
        postRankTreeMap.put(10l, treeSet);
        treeSet = new TreeSet<Long>();
        commentRankTreeMap.put(10l, treeSet);
        iij_timestamp =((VariableExpressionExecutor) expressionExecutors[0]).getAttribute().getName();
        ts =((VariableExpressionExecutor) expressionExecutors[1]).getAttribute().getName();
        postID =((VariableExpressionExecutor) expressionExecutors[2]).getAttribute().getName();
        commentID =((VariableExpressionExecutor) expressionExecutors[3]).getAttribute().getName();
        commentRepliedID = ((VariableExpressionExecutor) expressionExecutors[4]).getAttribute().getName();
        isPostFlag = ((VariableExpressionExecutor) expressionExecutors[5]).getAttribute().getName();

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
