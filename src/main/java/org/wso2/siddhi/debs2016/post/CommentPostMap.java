package org.wso2.siddhi.debs2016.post;

import java.util.HashMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class CommentPostMap {


    public static final long DURATION =  86400000L;
    public static final int INITIAL_SCORE = 10;
    private HashMap<Long, Long> commentToPostMap = new HashMap<>();

    /**
     * Adding a comment to a post
     * @param commentId the comment id
     * @param postId the post id
     */
    public void addCommentToPost(Long commentId, Long postId){
        commentToPostMap.put(commentId, postId);
    }

    /**
     * Adding a comment to a post
     * @param commentId the comment id
     * @param parentCommentId the parent comment id
     * @return ID of the parent post 
	 */
    public long addCommentToComment(Long commentId, Long parentCommentId){
        long parentPostId = commentToPostMap.get(parentCommentId);
        commentToPostMap.put(commentId, parentPostId);
        return parentPostId;
    }

}
