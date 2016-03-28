package org.wso2.siddhi.debs2016.post;

import com.google.common.collect.*;
import org.wso2.siddhi.debs2016.comment.Comment;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;

import java.util.*;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class PostStore {

    private HashMap<Long, Post> postList  = new HashMap<Long, Post> ();


    /**
     * Adds a new post to the Store
     * @param postId of new post
     * @param ts of post
     * @param userName of person who posted
     */
    public void addPost(Long postId, Long ts, String userName){
        postList.put(postId, new Post(ts, userName));
    }

    /**
     * Adds a comment the to the post
     *
     * @param postID the post id
     * @param commentID the comment id
     */
    public void addComment(long postID, long commentID, long ts)
    {

        //TODO Do we need to check if the post exists before adding the comment to it?
        getPost(postID).addComment(commentID, ts);
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

    /**
     * Update post store
     *
     * @param ts time ts
     */
    public void update(long ts) {

        int commentsScore = 0;
        long key ;
        Post post ;
        int score ;

        for(Iterator<Map.Entry<Long, Post>> it = postList.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Post> entry = it.next();
            key = entry.getKey();
            post = entry.getValue();
            score = post.update(ts);
            if (score <= 0)
            {
                postList.remove(key, post);
            }
        }
    }


    public void printTopPosts ()
    {


    }




}
