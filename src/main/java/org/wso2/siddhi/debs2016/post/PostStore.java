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
     * Gets the post store map (written to support unit testing)
     *
     */
    public TreeMultimap<Long, Post>   PostStoreMap()
    {
        return postList;
    }

    /**
     * The comparator for comparing post scores
     *
     */
    public class PostScoreComparator implements Comparator<Post>
    {

        @Override
        public int compare(Post post1, Post post2) {
            int score1 = post1.getScore();
            int score2 = post2.getScore();
            if (score1 > score2) {
                return 1;

            } else if (score2 > score1) {
                return -1;

            } else {

                if(post1.getArrivalTime() < post2.getArrivalTime())
                {
                    return 1;
                }else if (post2.getArrivalTime() < post1.getArrivalTime())
                {
                    return -1;
                }else
                {
                    //TODO: check the arrival time of comments
                    return 0;

                }
            }
        }
    }

}
