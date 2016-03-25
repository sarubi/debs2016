package org.wso2.siddhi.debs2016.post;

import java.util.HashMap;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class PostStore {

    private HashMap<Long, Post> postList = new HashMap<>();

    /**
     * Adds a new post to the Store
     * @param postId of new post
     * @param ts of post
     * @param user_name of person who posted
     */
    public void addPost(Long postId, Long ts, String user_name){
        postList.put(postId, new Post(ts, user_name));
    }


    /**
     * Get the post details relating to a post ID
     *
     * @param postId of post required
     * @return the Post object related to postId
     */
    public Post getPost(Long postId){
        return postList.get(postId);
    }

}
