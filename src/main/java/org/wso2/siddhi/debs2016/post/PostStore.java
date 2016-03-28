package org.wso2.siddhi.debs2016.post;

import com.google.common.collect.*;
import org.wso2.siddhi.debs2016.comment.Comment;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class PostStore {

    private HashMap<Long, Post> postList  = new HashMap<Long, Post> (); //postID, PostObject
    private TreeMultimap<Long, Post> postRanking = TreeMultimap.create(Comparator.reverseOrder(), new PostComparator()); //Score, PostObject
    private Long[] topThree = new Long[3];

    /**
     * Adds a new post to the Store
     * @param postId of new post
     * @param ts of post
     * @param userName of person who posted
     */
    public void addPost(Long postId, Long ts, String userName){
        postList.put(postId, new Post(ts, userName, postId));
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

//    /**
//     * Update post store
//     *
//     * @param ts time ts
//     */
//    public void update(long ts) {
//
//        int commentsScore = 0;
//        long key ;
//        Post post ;
//        int score ;
//
//        for(Iterator<Map.Entry<Long, Post>> it = postList.entrySet().iterator(); it.hasNext(); ) {
//            Map.Entry<Long, Post> entry = it.next();
//            key = entry.getKey();
//            post = entry.getValue();
//            score = post.update(ts);
//            if (score < 0)
//            {
//                it.remove();
//            }
//        }
//    }


    public void printTopPosts ()
    {


    }

    /**
     *
     * @param ts is the timestamp of event that might trigger a change
     */
    public void printTopThreePosts(Long ts){
        postRanking.clear();
        for(Iterator<Map.Entry<Long, Post>> it = postList.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Post> entry = it.next();
            long postId = entry.getKey();
            Post post = postList.get(postId);
            long postScore = post.update(ts);
            if (postScore <= 0){
                it.remove();
            }else{
                postRanking.put(postScore, post);
            }
        }


        int i = 0;
        boolean changeFlag = false;
        for (Post topPosts: postRanking.values()) {
            long postId = topPosts.getPostId();
            if (topThree[i] == null || !((topThree[i]).equals(postId))){
                changeFlag = true;
                topThree[i] = postId;
            }
            i++;
            if (i == 3){
                break;
            }
        }
        for (int j = i; j < 3; j++){
            if (topThree[j] == null || !(topThree[j]).equals(0L)){
                changeFlag = true;
                topThree[j] = 0L;
            }
        }

        if (changeFlag){
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            df.setTimeZone(TimeZone.getTimeZone("GMT"));
            String fmm = df.format(new java.util.Date(ts));
            System.out.print(fmm + ", ");
            for (int k = 0; k < 3 ; k++){
                if (topThree[k] != 0){
                    Post post = postList.get(topThree[k]);
                    System.out.print(topThree[k] + ", ");
                    System.out.print(postList.get(topThree[k]).getUserName() + ", ");
                    System.out.print(post.getScore() + ", ");
                    System.out.print(post.getNumberOfCommenters());
                }else{
                    System.out.print("-, -, -, -");
                }
                if(k != 2){
                    System.out.print(", ");
                }
            }
            System.out.println();
        }
    }


}

class PostComparator implements Comparator<Post>{

    @Override
    public int compare(Post post_1, Post post_2) {
        Long ts_1 = post_1.getArrivalTime();
        Long ts_2 = post_2.getArrivalTime();

        if (ts_1 > ts_2){
            return 1;
        } else if (ts_1 < ts_2){
            return -1;
        } else {

            Long ts_comment_1 = post_1.getLatestCommentTime();
            Long ts_comment_2 = post_2.getLatestCommentTime();

            if (ts_comment_1 > ts_comment_2){
                return 1;
            }else if (ts_comment_1 < ts_comment_2){
                return -1;
            }
        }
        return 1;
    }
}