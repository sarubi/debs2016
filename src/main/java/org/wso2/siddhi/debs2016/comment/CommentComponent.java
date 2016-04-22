package org.wso2.siddhi.debs2016.comment;

class CommentComponent {
    private final long timestamp;
    private final long commentId;

    /**
     * Constructor to create commentComponent object for Query2
     * @param timestamp is time of arrival of the comment
     * @param commentId is the ID of the comment
     */
    public CommentComponent(long timestamp, long commentId) {
        this.timestamp = timestamp;
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
     * @return timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

}