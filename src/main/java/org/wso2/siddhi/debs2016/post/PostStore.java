package org.wso2.siddhi.debs2016.post;

import com.google.common.collect.*;
import edu.ucla.sspace.util.BoundedSortedMultiMap;

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

    private HashMap<Long, Post> postList  = new HashMap<Long, Post> (5000); //postID, PostObject
    private BoundedSortedMultiMap<Integer, Long> postScoreMap = new BoundedSortedMultiMap<Integer, Long>(3, true, true, true);
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

    /**
     * The constructor
     *
     */
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
    public Post addPost(Long postId, Long ts, String userName){
        Post post = new Post(ts, userName, postId);
        postList.put(postId, post);
        return post;
    }

    /**
     * Get the post object of a given post ID
     *
     * @param postId the post id
     * @return the Post object related to postId
     */
    public Post getPost(Long postId){
        return postList.get(postId);
    }

    /**
     *
     * Gets map which contains the top three posts
     *
     */
    public BoundedSortedMultiMap<Integer, Long> getPostScoreMap()
    {
        return postScoreMap;
    }

    /**
     * Gets the map which contains the top three posts
     *
     * @return the map with top three scores
     */
    private TreeMultimap<Integer, Post>  getTopThreePostsMap() {

        TreeMultimap<Integer, Post> topScoreMap = TreeMultimap.create(Comparator.reverseOrder(), new PostComparator());

        Iterator itr = postScoreMap.entrySet().iterator();
        for (Iterator<Map.Entry<Integer, Long>> it = itr; it.hasNext(); ) {
            Map.Entry<Integer, Long> entry = it.next();
            int score = entry.getKey();
            long id = entry.getValue();
            topScoreMap.put(score, postList.get(id));
        }

        return topScoreMap;
    }

    /**
     * Indicates if the top three posts have changed or not
     *
     * @return true if they have changed false otherwise
     */
    public boolean hasTopThreeChanged()
    {

        TreeMultimap<Integer, Post> topScoreMap = getTopThreePostsMap();
        boolean changeFlag = false;
        int i = 0;

        for (Post post: topScoreMap.values()) {
            long id = post.getPostId();
            if (previousOrderedTopThree[i] == null || previousOrderedTopThree[i] != id){
                changeFlag = true;
                previousOrderedTopThree[i] = id;
            }
            i++;
            if (i == 3){
                break;
            }
        }

        for (; i < 3; i++){
            if (previousOrderedTopThree[i] != null){
                previousOrderedTopThree[i] = null;
                changeFlag = true;
            }
        }
        return changeFlag;
    }


    /**
     *
     * Print/write top three posts to a file
     *
     * @param ts is the timestamp of event that might trigger a change
     * @param printComments print output
     * @param writeToFile write the output to a file
     * @param delimiter the delimiter to use
     */

    public long printTopThreeComments(Long ts, boolean printComments, boolean writeToFile, String delimiter) {

        try{
                builder.setLength(0);
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                String fmm = df.format(new java.util.Date(ts));
                builder.append(fmm + delimiter);
                for (int k = 0; k < 3 ; k++){
                    if (this.previousOrderedTopThree[k] != null){
                        Post post = postList.get(this.previousOrderedTopThree[k]);
                        builder.append(this.previousOrderedTopThree[k] + delimiter);
                        builder.append(postList.get(this.previousOrderedTopThree[k]).getUserName() + delimiter);
                        builder.append(post.getTotalScore() + delimiter);
                        builder.append(post.getNumberOfCommenters());
                    }else{
                        builder.append("-,-,-,-");
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
        }catch (IOException ex){
            ex.printStackTrace();
        }
        return System.currentTimeMillis();
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

