package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.post.CommentPostMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class Comment {

    private long timeStamp;
    private int score;

    /**
     * Update score of comment based on current time
     * @param ts is timestamp of latest event
     */
    public void updateScore(Long ts){
        score = score - (int) ((ts - timeStamp)/ CommentPostMap.DURATION);
    }

    /**
     * Constructor to create new comment
     * @param ts is timestamp of comment
     */
    public Comment(long ts) {
        this.timeStamp = timeStamp;
        this.score = 10;
    }

    /**
     * Gets score of the comment
     *
     * @return the comment score
     */
    public long getScore()
    {
        return score;
    }

    /**
     *
     * @return true if the score is zero, false otherwise
     */
    public boolean isZero()
    {
        return score==0;
    }
}
