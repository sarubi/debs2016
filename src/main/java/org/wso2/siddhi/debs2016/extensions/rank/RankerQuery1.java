package org.wso2.siddhi.debs2016.extensions.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.input.CommentRecord;
import org.wso2.siddhi.debs2016.input.PostRecord;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class RankerQuery1 extends StreamFunctionProcessor {
    private HashMap<Long, TreeSet<Long>> commentsForPostMap; //Key is a post_id, values is the list of comment ids posted to that post
    private TreeMap<Long, TreeSet<Long>> postRankTreeMap; //Key is the total score of the post, value is the list of post ids which has that particular total score
    private TreeMap<Long, TreeSet<Long>> commentRankTreeMap; //Key is the total score of the comment, value is the list of comment ids which has that particular total score
    private HashMap<Long, PostRecord> posts;
    private HashMap<Long, CommentRecord> comments;

    private String iij_timestamp;
    private String ts;
    private String postID;
    private String commentID;
    private String commentRepliedID;
    private String isPostFlag;
    private long[] topThreePosts;
    private long startiij_timestamp;
    private long endiij_timestamp;
    private long count;

    private long MILISECONDS_FOR_DAY = 24 * 3600 * 1000;

    @Override
    protected Object[] process(Object[] objects) {
        //iij_timestamp, ts, post_id, -1 as comment_id, -1 as comment_replied_id
        try {
            long iij_timestamp = (Long) objects[0];
            endiij_timestamp = iij_timestamp;
            long ts = (Long) objects[1];
            long post_id = (Long) objects[2];
            long comment_id = (Long) objects[3]; //In the case of a post this field becomes user_id
            long comment_replied_id = (Long) objects[4];
            long user_id = (Long) objects[5];
            String user = (String) objects[6];
            boolean isPostFlag = (Boolean) objects[7];

            if (ts == -1l) {
                //This is the place where time measuring starts.
                startiij_timestamp = iij_timestamp;
                return new Object[]{""};
            }

            if (ts == -2l) {
                //This is the place where time measuring ends.
                showFinalStatistics();
                return new Object[]{""};
            }

            count++;

            //For each incoming post or comment we have to add them to the appropriate data structure with their initial scores
            if (isPostFlag) { //This is a new post
                //By default we know that all the post ids are new ones
                TreeSet<Long> treeSet = new TreeSet<Long>(); //Since this is post only, initial list of comments will be zero.
                commentsForPostMap.put(post_id, treeSet);
                treeSet = postRankTreeMap.get(10l);
                treeSet.add(post_id);//Initially the total score of the new post is 10.

                posts.put(post_id, new PostRecord(post_id, ts, user_id, 10, user)); //In this case comment_id carries the user_id
            } else {
                TreeSet<Long> treeSet = null;

                treeSet = commentRankTreeMap.get(10l);
                treeSet.add(comment_id);//Initially the total score of the new comment is 10.
                comments.put(comment_id, new CommentRecord(comment_id, ts, 10, user_id));

                if (post_id != -1 && comment_replied_id == -1l) {
                    TreeSet<Long> treeSet1 = commentsForPostMap.get(post_id);
                    if (treeSet1 == null) {
                        treeSet1 = new TreeSet<Long>();
                        commentsForPostMap.put(post_id, treeSet1);
                    }
                    treeSet1.add(comment_id);
                } else if (post_id == -1 && comment_replied_id != -1l) {
                    Iterator<Map.Entry<Long, TreeSet<Long>>> iterator = commentsForPostMap.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<Long, TreeSet<Long>> item = iterator.next();
                        Iterator<Long> commentsItr = item.getValue().iterator();

                        if (item.getValue().contains(comment_replied_id)) {
                            item.getValue().add(comment_id);
                            break;
                        }
                    }
                }
            }

            //Next, based on the timestamp value of this event object, we have to reduce the scores of the posts and the
            // comments which are already stored in the
            //two linked lists
            Iterator<Map.Entry<Long, PostRecord>> postsIterator = posts.entrySet().iterator();
            PostRecord pst = null;

            while (postsIterator.hasNext()) {
                pst = postsIterator.next().getValue();
                int scoreReduction = (int) ((ts - pst.ts) / MILISECONDS_FOR_DAY);

                if (scoreReduction < 10) {
                    pst.score = 10 - scoreReduction;
                } else {
                    //System.out.println("scoreReduction : " + scoreReduction);
                    pst.score = 0;
                }
            }

            Iterator<Map.Entry<Long, CommentRecord>> commentsIterator = comments.entrySet().iterator();
            CommentRecord comment = null;

            while (commentsIterator.hasNext()) {
                comment = commentsIterator.next().getValue();
                //Per each day we need to reduce the score by one. After passing ten days the score becomes negative value
                int scoreReduction = (int) ((ts - comment.ts) / MILISECONDS_FOR_DAY);

                if (scoreReduction < 10) {
                    comment.score = 10 - scoreReduction;
                } else {
                    comment.score = 0;
                }
            }
            //Next, we need to calculate total score of each post and then decide whether we should discard posts.
            //We need to do this for each and every post
            postsIterator = posts.entrySet().iterator();

            while (postsIterator.hasNext()) {
                pst = postsIterator.next().getValue();

                TreeSet<Long> commentorUsersList = new TreeSet<Long>();
                long totalScore = pst.score;
                TreeSet<Long> commentsForPost = commentsForPostMap.get(pst.post_id);
                Iterator<Long> itr = commentsForPost.iterator();

                while (itr.hasNext()) {
                    long commentID = itr.next();
                    commentorUsersList.add(comments.get(commentID).user_id);
                    totalScore += comments.get(commentID).score;
                }

                if (pst.totalScore != totalScore) {
                    //First, we need to remove the Post object from the existing list
                    TreeSet<Long> treeSet = postRankTreeMap.get(pst.totalScore);
                    if (treeSet != null) {
                        treeSet.remove(pst.post_id);
                    }

                    //Do this only if the total soce of the post has not reached zero.
                    if (totalScore != 0) {
                        //Next, we need to add the new score along with the post to the tree
                        treeSet = postRankTreeMap.get(totalScore);
                        if (treeSet == null) {
                            treeSet = new TreeSet<Long>();
                        }
                        treeSet.add(pst.post_id);
                        postRankTreeMap.put(totalScore, treeSet);
                        pst.totalScore = totalScore;
                    } else {
                        postsIterator.remove();
                        commentsForPostMap.remove(pst.post_id);
                        TreeSet<Long> treeSet2 = postRankTreeMap.get(pst.totalScore);
                        treeSet2.remove(pst.post_id);

                        if (treeSet2.size() == 0) {
                            postRankTreeMap.remove(pst.totalScore);
                        }
                    }
                }

                pst.numberOfCommentors = commentorUsersList.size();
            }

            if (updateRanks()) {
//            <ts,top1_post_id,top1_post_user,top1_post_score,top1_post_commenters,
//                    top2_post_id,top2_post_user,top2_post_score,top2_post_commenters,
//                    top3_post_id,top3_post_user,top3_post_score,top3_post_commenters>
                Object[] result = new Object[13];//There are thirteen fields in this record
                int counter = 0;
                StringBuilder sb = new StringBuilder();
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                String fmm = df.format(new java.util.Date(ts));
                sb.append(fmm);

                while (counter <= 2) {
                    long postID = topThreePosts[counter];

                    if (postID != -1l) {
                        sb.append("," + postID);
                        PostRecord currentPostRecord = posts.get(postID);
                        sb.append("," + currentPostRecord.user);
                        sb.append("," + currentPostRecord.totalScore);
                        sb.append("," + currentPostRecord.numberOfCommentors);
                    } else {
                        sb.append(",-,-,-,-");
                    }
                    counter++;
                }

                System.out.println(sb.toString());

                return new Object[]{sb.toString()};
            }
        }catch(Exception e){
            e.printStackTrace();
        }

        return null;
    }

    private boolean updateRanks() throws Exception{
        boolean result = false;
        Iterator<Long> itr = postRankTreeMap.descendingKeySet().iterator();
        int p = 0;
        while(itr.hasNext()){
            Long item = itr.next();

            TreeSet<Long> postIDs = postRankTreeMap.get(item);
            Iterator<Long> itr2 = postIDs.descendingIterator();

            while(itr2.hasNext()) {
                long pstID = itr2.next();
                if (topThreePosts[p] != pstID) {
                    result = true;
                    topThreePosts[p] = pstID;
                }

                p++;
                if (p > 2) {//We need only the top three posts from the postRankTreeMap
                    break;
                }
            }
            if (p > 2) {//We need only the top three posts from the postRankTreeMap
                break;
            }
        }

        if(p==2){
            topThreePosts[2] = -1;
            result = true;
        }else if(p==1){
            topThreePosts[1] = -1;
            topThreePosts[2] = -1;
            result = true;
        }else if((p==0)&&(topThreePosts[1]== -1)) {
            topThreePosts[0] = -1;
            topThreePosts[1] = -1;
            topThreePosts[2] = -1;
            result = true;
        }
        return result;
    }

    @Override
    protected Object[] process(Object o) {
        return null;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (expressionExecutors.length != 8) {
            System.err.println("Required Parameters : Six");
            return null;
        }

        topThreePosts = new long[3];
        postRankTreeMap = new TreeMap<Long, TreeSet<Long>>();
        commentsForPostMap = new HashMap<Long, TreeSet<Long>>();
        commentRankTreeMap = new TreeMap<Long, TreeSet<Long>>();
        posts = new HashMap<Long, PostRecord>();
        comments = new HashMap<Long, CommentRecord>();

        //By default we know that all the posts entered to the score map has a default score of 10
        TreeSet<Long> treeSet = new TreeSet<Long>();
        postRankTreeMap.put(10l, treeSet);
        treeSet = new TreeSet<Long>();
        commentRankTreeMap.put(10l, treeSet);

        for(int i=0; i < 3; i++){
            topThreePosts[i] = -1l;
        }

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
        System.out.flush();
    }
}