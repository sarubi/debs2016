package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.post.CommentPostMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class Comment {

    private long arrivalTime;


    /**
     * Constructor to create new comment
     * @param ts is timestamp of comment
     */
    public Comment(long ts) {
        this.arrivalTime = ts;
    }

    /**
     * Gets score of the comment at time ts
     *
     * @return the comment score
     */
    public int getScore(long ts)
    {

        int score = CommentPostMap.INITIAL_SCORE - (int) ((ts - arrivalTime)/ CommentPostMap.DURATION);
        return score> 0 ? score: 0;
    }

    /**
     * Gets the arrival time of the comment
     *
     * @return the arrival time
     */
    public long getArrivalTime()
    {
        return arrivalTime;
    }


}
