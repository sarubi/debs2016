package org.wso2.siddhi.debs2016.post;

import org.wso2.siddhi.debs2016.comment.Comment;

import java.util.HashMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class Post {

    private long timeStamp ;
    private int score;
    private String user_name;
    private HashMap<Long, Comment> commentList;

    /**
     * Constructor to create new post
     * @param timeStamp
     * @param user_name
     */
    public Post(long timeStamp, String user_name) {
        this.timeStamp = timeStamp;
        this.user_name = user_name;
        this.score = 10;
    }


    /**
     * Update score of post based on current time
     * @param ts
     */
    public void updateScore(Long ts){

    }
}
