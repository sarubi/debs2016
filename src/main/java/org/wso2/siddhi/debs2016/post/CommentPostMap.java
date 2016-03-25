package org.wso2.siddhi.debs2016.post;

import java.util.HashMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class CommentPostMap {

    private HashMap<Long, Long> commentToPostMap;
    public static long DURATION =  86400000l;

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
     * @param parentCommentId the pararent comment id
     */
    public void addCommentToComment(Long commentId, Long parentCommentId){

    }

}
