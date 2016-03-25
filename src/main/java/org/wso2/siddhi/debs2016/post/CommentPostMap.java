package org.wso2.siddhi.debs2016.post;

import java.util.HashMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class CommentPostMap {

    private HashMap<Long, Long> commentToPostMap = new HashMap<>();


    /**
     * Adding a comment to a post
     * @param commentId the comment id
     * @param postId the post id
     */
    public void addCommentToPost(Long commentId, Long postId){

    }

    /**
     * Adding a comment to a post
     * @param commentId the comment id
     * @param parentCommentId the parent comment id
     * @return ID of the parent post 
	 */
    public long addCommentToComment(Long commentId, Long parentCommentId){

        return 1;

    }

}
