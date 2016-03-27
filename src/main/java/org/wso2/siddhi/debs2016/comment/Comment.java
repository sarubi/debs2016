package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.post.CommentPostMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class Comment {

    private long timeStamp;


    /**
     * Constructor to create new comment
     * @param ts is timestamp of comment
     */
    public Comment(long ts) {
        this.timeStamp = timeStamp;
    }

    /**
     * Gets score of the comment at time ts
     *
     * @return the comment score
     */
    public int getScore(long ts)
    {

        int score = CommentPostMap.INITIAL_SCORE - (int) ((ts - timeStamp)/ CommentPostMap.DURATION);
        return score> 0 ? score: 0;
    }


}
