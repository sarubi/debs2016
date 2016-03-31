package org.wso2.siddhi.debs2016.comment;

/**
 * Created by bhagya on 3/31/16.
 */
public class CommentComponent {
    private long ts;
    private long commentId;

    public CommentComponent(long ts, long commentId) {
        this.ts = ts;
        this.commentId = commentId;
    }

    public long getCommentId() {
        return commentId;
    }

    public long getTs() {
        return ts;
    }

    public void setCommentId(long commentId) {
        this.commentId = commentId;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
