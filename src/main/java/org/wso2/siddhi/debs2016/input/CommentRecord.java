package org.wso2.siddhi.debs2016.input;

public class CommentRecord {
    public long ts; //Timestamp of the comment
    public long comment_id;
    public int score;

    public CommentRecord(long comment_id, long ts, int score){
        this.ts = ts;
        this.comment_id = comment_id;
        this.score = score;
    }
}
