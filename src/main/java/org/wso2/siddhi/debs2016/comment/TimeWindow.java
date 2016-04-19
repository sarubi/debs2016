package org.wso2.siddhi.debs2016.comment;

import edu.ucla.sspace.util.BoundedSortedMultiMap;
import org.wso2.siddhi.debs2016.Processors.Q1EventManager;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by anoukh on 3/29/16.
 */
public class TimeWindow {


    private LinkedBlockingQueue<CommentPostComponent> oneDay = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> twoDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> threeDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> fourDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> fiveDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> sixDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> sevenDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> eightDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> nineDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentPostComponent> tenDays = new LinkedBlockingQueue<>();
    private PostStore postStore;
    BoundedSortedMultiMap<Integer, Long> postScoreMap;
    CommentPostMap commentPostMap;

    /**
     * The constructor
     * @param postStore the post score object
     * @param commentPostMap the comment post map object
     */
    public TimeWindow(PostStore postStore, CommentPostMap commentPostMap)
    {
        this.postStore = postStore;
        this.postScoreMap = postStore.getPostScoreMap();
        this.commentPostMap = commentPostMap;
    }


    /**
     * Register a new comment in the Time Window
     * @param post is the post object that received the new comment
     * @param ts is the time of arrival of the new comment
     */
    public void addComment(Post post, long ts, long commenter_id){
        long postId = post.getPostId();

        oneDay.add(new CommentPostComponent(post, ts, commenter_id, false));
        postScoreMap.remove(post.getTotalScore(), postId);
        post.addComment(ts, commenter_id);
        postScoreMap.put(post.getTotalScore(), postId);
    }

    /**
     *
     * Add a new post to the post window
     *
     * @param post the new post
     */
    public void addNewPost(long ts, Post post){
        oneDay.add(new CommentPostComponent(post, ts, 0, true));
        postScoreMap.put(10, post.getPostId());
    }

    /**
     * Move the comments and posts along the time axis
     * @param ts time stamp
     */
    public boolean updateTime(long ts){

        process(ts, oneDay, twoDays, 1);
        process(ts, twoDays, threeDays, 2);
        process(ts, threeDays, fourDays, 3);
        process(ts, fourDays, fiveDays, 4);
        process(ts, fiveDays, sixDays, 5);
        process(ts, sixDays, sevenDays, 6);
        process(ts, sevenDays, eightDays, 7);
        process(ts, eightDays, nineDays, 8);
        process(ts, nineDays, tenDays, 9);
        return process(ts, tenDays, null, 10);

    }

    /**
     * Processes a given time window
     *
     * @param ts the new event time
     * @param queue the window iterator
     * @param queueNumber the window number
     */
    private boolean process(long ts, LinkedBlockingQueue<CommentPostComponent> queue, LinkedBlockingQueue<CommentPostComponent> nextQueue, int queueNumber) {
        try {
            HashMap<Long, Post> postMap = postStore.getPostList();
            Iterator<CommentPostComponent> iterator = queue.iterator();

            while (iterator.hasNext()) { //Iterate over Queue
                CommentPostComponent commentPostObject = iterator.next();
                long objectArrivalTime = commentPostObject.getTs();
                if (objectArrivalTime <= (ts - CommentPostMap.DURATION * queueNumber)) {
                    Post post = commentPostObject.getPost();
                    long postID = post.getPostId();
                    int oldScore = post.getTotalScore();
                    postScoreMap.remove(oldScore, postID);
                    post.decrementTotalScore();
                    int newScore = post.getTotalScore();
                    if (postStore.getPostList().containsKey(postID)) {
                        boolean isPost = commentPostObject.isPost();
                        commentPostObject.setExpiringTime(commentPostObject.getExpiringTime() + CommentPostMap.DURATION);
                        if (newScore <= 0) {
                            postMap.remove(postID);
                            commentPostMap.getCommentToPostMap().remove(postID);
                            Q1EventManager.timeOfEvent = commentPostObject.getExpiringTime();
                        } else {
                            postScoreMap.put(newScore, postID);
                            if (nextQueue != null) {
                                nextQueue.add(commentPostObject);
                            } else {
                                if (!isPost) {
                                    post.removeCommenter(commentPostObject.getUserID());
                                }
                            }
                        }
                    }
                    iterator.remove();
                } else {
                    break;
                }
            }
            if (nextQueue == null) {
                return postStore.hasTopThreeChanged();
            } else {
                return false;
            }
        } catch (java.lang.Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
