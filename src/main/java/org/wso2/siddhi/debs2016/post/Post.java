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
    public static long DURATION =  86400000l;
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
     * Update score of post based on current time
     * @param ts the time stamp
     */
    public void updateScore(Long ts){
        updatePostScore(ts);
        updateComments(ts);

    }

    /**
     * Update the post score (i.e. total score)
     *
     * @param ts the update time
     */
    private void updatePostScore(long ts)
    {
        score = score - (int) ((ts - timeStamp)/DURATION);
    }

    /**
     * Update post comment
     *
     * @param ts the updat time
     */
    private void updateComments(long ts)
    {
        for(Iterator<Map.Entry<Long, Comment>> it = commentList.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Comment> entry = it.next();
            long key = entry.getKey();
            Comment comment = commentList.get(key);
            comment.updateScore(ts);
            //TODO: if score is 0 then delete the comment from post
        }

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
