package org.wso2.siddhi.debs2016.input;

/**
 * Created by miyurud on 2/12/16.
 */
public class PostRecord {
    public long post_id;
    public long ts; //Timestamp of the post
    public long ts_last_recv; //Timestamp of the last received comment.
}
