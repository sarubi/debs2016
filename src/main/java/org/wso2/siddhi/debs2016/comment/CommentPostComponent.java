package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.post.Post;

/**
 * An Object to record the timestamp of the comment of a post
 * Created by aaw on 4/1/16.
 */
class CommentPostComponent {
    private final long ts;
    private long expiringTime;
    private final Post post;
    private final long userID;
    private final boolean isPost;


    /**
     * Constructor to create CommentPostComponent for Query1 TimeWidow
     *
     * @param post the post
     * @param ts the time stamp
     * @param userId the user ID of the user who commented on the post
     * @param isPost true if its a post object, false if is a comment object
     */
    public CommentPostComponent(Post post, long ts, long userId, boolean isPost) {
        this.ts = ts;
        this.post = post;
        this.userID = userId;
        this.isPost = isPost;
        this.expiringTime = ts;

    }

    /**
     * Getter for userID variable
     * @return userID
     */
    public long getUserID() {
        return userID;
    }

    /**
     * Getter for isPost variable
     * @return true if object is Post object, false if it is Comment object
     */
    public boolean isPost() {
        return isPost;
    }

    /**
     * Getter for expiringTime variable
     * @return the time stamp of what time the object expires
     */
    public long getExpiringTime() {
        return expiringTime;
    }

    /**
     * Setter for expiringTime variable
     * @param expiringTime is the time that the object is expected to expire
     */
    public void setExpiringTime(long expiringTime) {
        this.expiringTime = expiringTime;
    }

    /**
     * Gets the post relating to he object
     * @return the post
     */
    public Post getPost() {
        return post;
    }

    /**
     * Gets the arrival time of the object
     * @return the comment arrival time
     */
    public long getTs() {
        return ts;
    }

}
