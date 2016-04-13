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
    private int totalScore;
    private String userName;
    private Set<Long> commenters = new HashSet<Long>();


    public void removeCommenter(long userID){
        commenters.remove(userID);
    }


    /**
     * Constructor to create new post
     * @param timeStamp the arrival time of the comment
     * @param userName the name of the user you created the comment
     */
    public Post(long timeStamp, String userName, Long postId) {
        this.arrivalTime = timeStamp;
        this.userName = userName;
        this.totalScore = 10;
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
     *
     * Get the previous total commentScore of a post before before updated for current timestamp
     *
     * @return Old commentScore of post
     */
    public int getTotalScore(){
        return totalScore;
    }

    /**
     * Decrement total score by one
     */
    public void decrementTotalScore(){
        totalScore = totalScore - 1;
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
        totalScore = totalScore + 10;
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
     * @return postId the post id
     */
    public long getPostId() {
        return postId;
    }
}
