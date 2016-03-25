package org.wso2.siddhi.debs2016.post;

import org.wso2.siddhi.debs2016.comment.Comment;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class Post {

    private long timeStamp ;
    private int score;
    private String userName;
    private HashMap<Long, Comment> commentList = new HashMap<Long, Comment>();

    /**
     * Constructor to create new post
     * @param timeStamp the arrival time of the comment
     * @param userName the name of the user you created the comment
     */
    public Post(long timeStamp, String userName) {
        this.timeStamp = timeStamp;
        this.userName = userName;
        this.score = 10;
    }


    /**
     * Gets the total score of the post at time ts
     *
     * @param ts time
     * @return the total score at time ts
     */
    public int getScore(long ts)
    {
        return getPostScore(ts) + getCommentsScore(ts);
    }

    /**
     * Update the post score (i.e. total score)
     *
     * @param ts the update time
     */
    private int getPostScore(long ts)
    {
        return CommentPostMap.INITIAL_SCORE - (int) ((ts - timeStamp)/CommentPostMap.DURATION);
    }


    /**
     * Gets to score of all comments at time ts
     *
     * @param ts the time
     * @return the score of all comments at time ts
     */
    private int getCommentsScore(long ts)
    {
        int commentsScore = 0;

        for(Iterator<Map.Entry<Long, Comment>> it = commentList.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Comment> entry = it.next();
            long key = entry.getKey();
            Comment comment = commentList.get(key);
            int commentScore = comment.getScore(ts);
            if(commentScore <= 0)
            {
                commentList.remove(key);
            }else {
                commentsScore = commentsScore + comment.getScore(ts);
            }
        }

        return commentsScore;
    }


    /**
     * Adds a new comment to a post
     *
     * @param commentID the commentID
     * @param comment the comment
     * @param arrivalTime the arrival time
     */
    public void addComment(Long commentID, String comment, Long arrivalTime)
    {
        commentList.put(commentID, new Comment(arrivalTime));
    }
}
