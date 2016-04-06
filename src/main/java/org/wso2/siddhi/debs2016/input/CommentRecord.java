package org.wso2.siddhi.debs2016.input;

/**
 * The comment record
 *
 */
public class CommentRecord {
    public long ts; //Timestamp of the comment
    public long comment_id;
    public int score; //This is the score which is determined by the ts and the ts value of the latest event.
    //The value of the score is only dteremined by the latest event received by the system.
    //It is in the range [0-10]
    public int totalScore; //This is the total score of this comment. This is determined by by the comments list
    //associated with this comment.
    public long user_id;    //User who posted this comment

    /**
     *
     * The constructor
     *
     * @param comment_id the comment_id
     * @param ts arrival time of the comment
     * @param score the score
     * @param user_id the user id
     */
    public CommentRecord(long comment_id, long ts, int score, long user_id){
        this.ts = ts;
        this.comment_id = comment_id;
        this.score = score;
        this.user_id = user_id;
    }
}