package org.wso2.siddhi.debs2016.input;

public class PostRecord {
    public long post_id;
    public long ts; //Timestamp of the post
    public int score; //This is the score which is determined by the ts and the ts value of the latest event.
    //The value of the score is only determined by the latest event received by the system.
    //It is in the range [0-10]
    public int totalScore; //This is the total score of this comment. This is determined by by the comments tree
    //associated with this post.
    public long user_id;    //User who posted this comment
    public String user; //This String contains the actual name of the user.
    public int numberOfCOmmentors; //The count of the commentors

    public PostRecord(long post_id, long ts, long user_id, int score, String user) {
        this.post_id = post_id;
        this.ts = ts;
        this.score = score;
        this.user_id = user_id;
        this.user = user;
        numberOfCOmmentors = 0;
    }
}