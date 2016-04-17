package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.post.Post;

/**
 * An Object to record the timestamp of the comment of a post
 * Created by aaw on 4/1/16.
 */
class CommentForPost {
    private long ts;
    private long expiringTime;
    private Post post;
    private long userID;
    private boolean isPost;

    /**
     * Setter for post variable
     * @param post is the Post Object
     */
    public void setPost(Post post) {
        this.post = post;
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
     * @return true if object is Post object
     */
    public boolean isPost() {
        return isPost;
    }

    /**
     * Getter for expiringTime variable
     * @return the value ts of when this object expired
     */
    public long getExpiringTime() {
        return expiringTime;
    }

    /**
     * Setter for expiringTime variable
     * @param expiringTime
     */
    public void setExpiringTime(long expiringTime) {
        this.expiringTime = expiringTime;
    }

    /**
     * Gets the post
     *
     * @return the post
     */
    public Post getPost() {
        return post;
    }

    /**
     * Gets the arrival time of the comment
     *
     * @return the comment arrival time
     */
    public long getTs() {
        return ts;
    }

    /**
     * The constructor
     *
     * @param post the post
     * @param ts   the time stamp
     */
    public CommentForPost(Post post, long ts, long userId, boolean isPost) {
        this.ts = ts;
        this.post = post;
        this.userID = userId;
        this.isPost = isPost;
        this.expiringTime = ts;

    }
}
