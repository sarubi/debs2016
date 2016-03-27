package org.wso2.siddhi.debs2016.post;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import org.wso2.siddhi.debs2016.comment.Comment;

import java.util.*;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class PostStore {

    private TreeMultimap<Long, Post> postList  = TreeMultimap.create(Ordering.arbitrary(), new PostScoreComparator());;

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
        getPost(postID).addComment(commentID, ts);
    }

    /**
     * Get the post details relating to a post ID
     *
     * @param postId of post required
     * @return the Post object related to postId
     */
    public Post getPost(Long postId){
        return postList.get(postId).first();
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
        for(Map.Entry<Long, Post> e : postList.entries()) {
            key = e.getKey();
            post = e.getValue();
            score = post.update(ts);
            if (score <= 0)
            {
                postList.remove(key, post);
            }
        }
    }

    /**
     * print the top 3 posts
     *
     */
    public void printTop3Posts()
    {

    }

    /**
     * The comparator for comparing post scores
     *
     */
    public class PostScoreComparator implements Comparator<Post>
    {

        @Override
        public int compare(Post s1, Post s2) {
            int score1 =  s1.getScore();
            int score2 = s2.getScore();
            if (score1 >= score2) {
                return score1;
            } else {
                return score2;
            }

        }
    }



}
