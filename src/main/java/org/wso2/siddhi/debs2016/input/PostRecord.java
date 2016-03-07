package org.wso2.siddhi.debs2016.input;

public class PostRecord {
    public long post_id;
    public long ts; //Timestamp of the post

    public PostRecord(long post_id, long ts) {
        this.post_id = post_id;
        this.ts = ts;
    }
}
