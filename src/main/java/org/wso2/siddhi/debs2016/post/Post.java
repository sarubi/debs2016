package org.wso2.siddhi.debs2016.post;

import org.wso2.siddhi.debs2016.comment.Comment;

import java.util.*;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class Post {

    private long arrivalTime;
    private long latestCommentTime;
    private long postId;

    private int score;
    private int oldScore;
    private String userName;
    private HashMap<Long, Comment> commentList = new HashMap<Long, Comment>(); //CommentId, CommentObject
    private Set<Long> commenters = new HashSet<Long>();

    /**
     * Constructor to create new post
     * @param timeStamp the arrival time of the comment
     * @param userName the name of the user you created the comment
     */
    public Post(long timeStamp, String userName, Long postId) {
        this.arrivalTime = timeStamp;
        this.userName = userName;
        // This is total score of comments of the post ??
        this.score = 0;
        oldScore = 10;
        this.postId = postId;
    }

    /**
     * Get the name of the user who created the post
     *
     * @return the user name
     */
    public String getUserName()
    {
        return userName;
    }

    /**
     * Gets the total score of the post at time ts
     *
     * @return the total score at time ts
     */
    public int getScore(long ts) {
        oldScore =  score + getPostScore(ts);
        return oldScore;
    }

    /**
     *
     * Get the previous total score of a post before before updated for current timestamp
     *
     * @return Old score of post
     */
    public int getOldScore(){
        return oldScore;
    }

    /**
     * Update the post score (i.e. total score)
     *
     * @param ts the update time
     */
    private int getPostScore(long ts) {
        return CommentPostMap.INITIAL_SCORE - (int) ((ts - arrivalTime)/CommentPostMap.DURATION);
    }

    /**
     * Adds a new comment to a post
     *
     * @param userID the ID of the user
     * @param arrivalTime the arrival time
     */
    public void addComment(Long arrivalTime, Long userID)
    {
        commenters.add(userID);
        latestCommentTime = arrivalTime;
        score = score + 10;
    }


    /**
     *
     * The arrival time of the post
     *
     */
    public long getArrivalTime()
    {
        return arrivalTime;
    }



    /**
     * Gets the comment from the comment id (added to support unit testing)
     *
     * @param commentID the comment id
     * @return the comment whose comment id = commentID
     */
    public Comment getComment(long commentID)
    {
        return commentList.get(commentID);

    }

    /**
     * Calculate number of users who have commented on a post
     *
     * @return number of commenters
     */
    public int getNumberOfCommenters(){
        return  commenters.size();
    }

    /**
     * Return the timestamp of the latest comment that arrived
     *
     * @return
     */
    public long getLatestCommentTime() {
        return latestCommentTime;
    }

    /**
     * Get the post ID of the post
     * @return postId
     */
    public long getPostId() {
        return postId;
    }

    /**
     * Decrement Score by one
     */
    public void decrementScore(){
        score = score - 1;
    }
}
