package org.wso2.siddhi.debs2016.comment;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class Comment {

    private long ts;
    private int score;


    /**
     * Update score of comment based on current time
     * @param ts is timestamp of latest event
     */
    public void updateScore(Long ts){

    }

    /**
     * Constructor to create new comment
     * @param ts is timestamp of comment
     */
    public Comment(long ts) {
        this.ts = ts;
        this.score = 10;
    }
}
