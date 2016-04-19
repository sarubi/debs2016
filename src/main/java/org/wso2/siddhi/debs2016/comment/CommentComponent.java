package org.wso2.siddhi.debs2016.comment;

/**
 * Created by bhagya on 3/31/16.
 */
class CommentComponent {
    private final long ts;
    private final long commentId;

    /**
     * Constructor to create commentComponent object for Query2
     * @param ts is time of arrival of the comment
     * @param commentId is the ID of the comment
     */
    public CommentComponent(long ts, long commentId) {
        this.ts = ts;
        this.commentId = commentId;
    }

    /**
     * Get the comment ID of the component
     * @return commentId
     */
    public long getCommentId() {
        return commentId;
    }

    /**
     * Get the time of arrival of the comment
     * @return ts
     */
    public long getTs() {
        return ts;
    }

}