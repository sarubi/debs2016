package org.wso2.siddhi.debs2016.comment;

import edu.ucla.sspace.util.BoundedSortedMultiMap;
import edu.ucla.sspace.util.SortedMultiMap;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;
import scala.util.control.Exception;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by anoukh on 3/29/16.
 */
public class TimeWindow {

    LinkedBlockingQueue<Post> noComments = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> oneDay = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> twoDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> threeDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> fourDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> fiveDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> sixDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> sevenDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> eightDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> nineDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> tenDays = new LinkedBlockingQueue<>();
    private PostStore postStore;
    BoundedSortedMultiMap<Integer, Long> postScoreMap;
    /**
     * The constructor
     * @param postStore the post score object
     */
    public TimeWindow(PostStore postStore)
    {
        this.postStore = postStore;
        this.postScoreMap = postStore.getPostScoreMap();
    }


    /**
     * Register a new comment in the Time Window
     * @param post is the post object that received the new comment
     * @param ts is the time of arrival of the new comment
     */
    public void addComment(Post post, long ts){
//        this.postStore = postStore;
        long postId = post.getPostId();
        noComments.remove(post);
        oneDay.add(new CommentForPost(post, ts));
        postScoreMap.remove(post.getOldScore(), postId);
        postScoreMap.put(post.getScore(ts), postId);
    }

    /**
     *
     * Add a new post with no comments
     *
     * @param post the new post
     */
    public void addNewPost(Post post){
        noComments.add(post);
    }

    /**
     * Move the comments along the time axis
     * @param ts time stamp
     */
    public void updateTime(long ts){

        process(ts, oneDay, twoDays, 1);
        process(ts, twoDays, threeDays, 2);
        process(ts, threeDays, fourDays, 3);
        process(ts, fourDays, fiveDays, 4);
        process(ts, fiveDays, sixDays, 5);
        process(ts, sixDays, sevenDays, 6);
        process(ts, sevenDays, eightDays, 7);
        process(ts, eightDays, nineDays, 8);
        process(ts, nineDays, tenDays, 9);
        process(ts, tenDays, null, 10);
        processPost(ts);

    }

    /**
     * Processes a given time window
     *
     * @param ts the new event time
     * @param queue the window iterator
     * @param queueNumber the window number
     */
    private void process(long ts,  LinkedBlockingQueue<CommentForPost> queue, LinkedBlockingQueue<CommentForPost> nextQueue, int queueNumber) {

        try {

            Iterator<CommentForPost> iterator = queue.iterator();
            HashMap<Long, Post> postMap = postStore.getPostList();

            while (iterator.hasNext()) { //Iterate over Queue
                CommentForPost commentPostObject = iterator.next();
                long commentTs = commentPostObject.getTs();
                if (commentTs <= (ts - CommentPostMap.DURATION * queueNumber)) {
                    Post post = commentPostObject.getPost();
                    long postID = post.getPostId();
//                    long oldPostScore = post.getScore(ts); //This is not the old score. Check next line
                    int oldPostScore = post.getOldScore();
                    if (nextQueue != null) {
                        nextQueue.add(commentPostObject);
                    }
                    post.decrementScore();
                    postScoreMap.remove(oldPostScore, postID);
                    int newScore = post.getScore(ts);
                    if(newScore <= 0)
                    {
                        postMap.remove(postID);
                    }
                    if(newScore > 0) {
                        postScoreMap.put(newScore, postID);
                    }

                    iterator.remove();
                } else {
                    break;
                }
            }
        } catch (java.lang.Exception e)
        {
            e.printStackTrace();
        }
    }

    private void processPost(long ts){
        for (Post post: noComments) {
            int oldScore = post.getOldScore();
            int newScore = post.getScore(ts);
            if (oldScore != newScore){
                postScoreMap.remove(oldScore, post.getPostId());
                if (newScore > 0){
                    postScoreMap.put(newScore, post.getPostId());
                }
            }
        }
    }

}
