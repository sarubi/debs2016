package org.wso2.siddhi.debs2016.post;

/**
 * Created by anoukh on 4/5/16.
 */
public class PostWindowObject {

    long arrivalTime;
    Post post;

    public PostWindowObject(long arrivalTime, Post post) {
        this.arrivalTime = arrivalTime;
        this.post = post;
    }

    public long getArrivalTime() {
        return arrivalTime;
    }

    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;
    }

    public Post getPost() {
        return post;
    }

}
