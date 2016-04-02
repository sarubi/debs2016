package org.wso2.siddhi.debs2016.post;

import com.google.common.collect.*;
import edu.ucla.sspace.util.BoundedSortedMultiMap;
import edu.ucla.sspace.util.SortedMultiMap;
import edu.ucla.sspace.util.TreeMultiMap;
import org.wso2.siddhi.debs2016.comment.MyLong;

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
    private BoundedSortedMultiMap<Long, Long> postScoreMap = new BoundedSortedMultiMap<Long, Long>(3, true, true, true);
    private Long[] previousOrderedTopThree = new Long[3];
    StringBuilder builder=new StringBuilder();
    private BufferedWriter writer;
    private File q1;
    private int count = 0;


    /**
     *
     * Gets the post list hash map
     *
     * @return the post list
     */
    public HashMap<Long, Post> getPostList()
    {
        return postList;
    }

    public PostStore(){
        q1= new File("q1.txt");
        try{
            writer = new BufferedWriter(new FileWriter(q1, true));
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Adds a new post to the Store
     * @param postId of new post
     * @param ts of post
     * @param userName of person who posted
     */
    public void addPost(Long postId, Long ts, String userName){
        postList.put(postId, new Post(ts, userName, postId));
        postScoreMap.put(10L, postId);
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
     *
     * Gets the postScoreMap
     *
     */
    public SortedMultiMap<Long, Long> getPostScoreMap()
    {
        return postScoreMap;
    }


    /**
     *
     * @param ts is the timestamp of event that might trigger a change
     */

    public void printTopThreeComments(Long ts) {

        SortedMultiMap<Long, Long> map = new TreeMultiMap<Long, Long>();
        TreeMultimap<Long, Post> topScoreMap = TreeMultimap.create(Comparator.reverseOrder(), new PostComparator());
        map = postScoreMap;
        int uniqueScoreCount = 0;
        int size = map.size();

        System.out.println(" post map size " + map.size());
        for (Iterator<Map.Entry<Long, Long>> it = postScoreMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, Long> entry = it.next();
            long score = entry.getKey();
            long id = entry.getValue();
            System.out.println("Score ID " + score + "  "  + id);
        }

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

    private long[][] updateTopThree(long ts){


        int removeCount = 0;
        long[][] topThree = {{0L,0L}, {0L,0L}, {0L,0L}}; //{postId, Score}
        PostComparator postComparator = new PostComparator();

        for(Iterator<Map.Entry<Long, Post>> it = postList.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Post> entry = it.next();
            long postId = entry.getKey();
            Post post = entry.getValue();
            long postScore = post.getScore(ts);
            if (postScore <= 0){
                it.remove();
                removeCount++;
            }

        }

     //   if(removeCount> 100) {
       //     System.out.println("remove count" + removeCount);
        //}
            return topThree;
    }


    private long[][] updateTopThreeOLDVersion(long ts){

        long[][] topThree = {{0L,0L}, {0L,0L}, {0L,0L}}; //{postId, Score}
        PostComparator postComparator = new PostComparator();

        for(Iterator<Map.Entry<Long, Post>> it = postList.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Long, Post> entry = it.next();
            long postId = entry.getKey();
            Post post = entry.getValue();
            long postScore = post.getScore(ts);
            if (postScore <= 0){
                it.remove();
            }else{
                for (int i = 0; i < 3 ; i++){
                    if (postScore == topThree[i][1]){
                        if (postComparator.compare(post, postList.get(topThree[i][0])) == 1){
                            long tempId = topThree[i][0];
                            long tempScore = topThree[i][1];

                            topThree[i][0] = postId;
                            topThree[i][1] = postScore;

                            if (i != 2){
                                long tempNextId = topThree[i+1][0];
                                long tempNextScore = topThree[i+1][1];
                                topThree[i+1][0] = tempId;
                                topThree[i+1][1] = tempScore;

                                if (i == 0){
                                    topThree[i+2][0] = tempNextId;
                                    topThree[i+2][1] = tempNextScore;
                                }
                            }
                            break;
                        }else {
                            continue;
                        }
                    }else if (postScore > topThree[i][1]){
                        long tempId = topThree[i][0];
                        long tempScore = topThree[i][1];

                        topThree[i][0] = postId;
                        topThree[i][1] = postScore;

                        if (i != 2){
                            long tempNextId = topThree[i+1][0];
                            long tempNextScore = topThree[i+1][1];
                            topThree[i+1][0] = tempId;
                            topThree[i+1][1] = tempScore;

                            if (i == 0){
                                topThree[i+2][0] = tempNextId;
                                topThree[i+2][1] = tempNextScore;
                            }
                        }
                        break;
                    }
                }
            }
        }
        return topThree;
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

