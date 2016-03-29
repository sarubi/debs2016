package org.wso2.siddhi.debs2016.post;

import com.google.common.collect.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by malithjayasinghe on 3/25/16.
 */
public class PostStore {

    private HashMap<Long, Post> postList  = new HashMap<Long, Post> (); //postID, PostObject
    private TreeMultimap<Long, Long> postRanking = TreeMultimap.create(Comparator.reverseOrder(), Comparator.naturalOrder()); //Score, PostId
    private TreeMultimap<Long, Post> sortedPostRanking = TreeMultimap.create(Comparator.reverseOrder(), new PostComparator()); //Score, PostObject
    private Long[] topThree = new Long[3];
    StringBuilder builder=new StringBuilder();
    private BufferedWriter writer;
    private File q1;

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

    public long writeTopThreeComments(String delimiter, boolean printComments, boolean writeToFile, Long ts) {
        q1 = new File("q1.txt");
        try{
            writer = new BufferedWriter(new FileWriter(q1, true));
        }catch (IOException e) {
            e.printStackTrace();
        }
        postRanking.clear();
        for(Iterator<Map.Entry<Long, Post>> it = postList.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Post> entry = it.next();
            long postId = entry.getKey();
            Post post = postList.get(postId);
            long postScore = post.update(ts);
            if (postScore <= 0){
                it.remove();
            }else{
                postRanking.put(postScore, postId);
            }
        }

        sortedPostRanking.clear();
        int loopCounter = 0;
        for(Iterator<Map.Entry<Long, Collection<Long>>> it = postRanking.asMap().entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Collection<Long>> entry = it.next();
            long score = entry.getKey();
            Collection<Long> postIdList = entry.getValue();

            for (Long postId: postIdList) {
                sortedPostRanking.put(score, postList.get(postId));
            }
            loopCounter++;
            if (loopCounter == 3){
                break;
            }
        }
        int i = 0;
        boolean changeFlag = false;
        for (Post topPosts: sortedPostRanking.values()) {
            if (topThree[i] == null || !((topThree[i]).equals(topPosts.getPostId()))){
                changeFlag = true;
                topThree[i] = topPosts.getPostId();
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
        try {
            if (changeFlag){
                builder.setLength(0);
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                String fmm = df.format(new java.util.Date(ts));
                builder.append(fmm + delimiter);
                for (int k = 0; k < 3 ; k++){
                    if (topThree[k] != 0){
                        Post post = postList.get(topThree[k]);
                        builder.append(topThree[k] + delimiter);
                        builder.append(postList.get(topThree[k]).getUserName() + delimiter);
                        builder.append(post.getScore() + delimiter);
                        builder.append(post.getNumberOfCommenters());
                    }else{
                        builder.append("-, -, -, -");
                    }
                    if(k != 2){
                        builder.append(delimiter);
                    }
                }
                builder.append("\n");
                if (printComments) {
                    System.out.print(builder.toString());
                }

                if (writeToFile) {
                    writer.write(builder.toString());
                }
                return System.currentTimeMillis();
            }
        }catch(IOException e)
        {
            e.printStackTrace();
        }
        return  -1L;
    }
    /**
     *
     * De-allocate resources
     *
     */

    public void destroy(){

        try {
            writer.close();
        }catch (IOException e)
        {
            e.printStackTrace();
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
        return -1;
    }

}