package org.wso2.siddhi.debs2016.post;

/**
 * Created by anoukh on 4/5/16.
 */
public class PostWindowObject {

    long arrivalTime;
    Post post;

    /**
     * The constructor
     *
     * @param arrivalTime the arrival time of the post
     * @param post the post object
     */
    public PostWindowObject(long arrivalTime, Post post) {
        this.arrivalTime = arrivalTime;
        this.post = post;
    }

    /**
     * Gets the arrival time of the post
     *
     * @return the arrival time
     */
    public long getArrivalTime() {
        return arrivalTime;

    }

    /**
     * Sets the arrival time of the post
     *
     * @param arrivalTime the arrival time of the post
     */
    public void setArrivalTime(long arrivalTime) {
        this.arrivalTime = arrivalTime;

    }

    /**
     * Gets the post object
     *
     * @return the post
     */
    public Post getPost() {
        return post;

    }

}
