/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

public class PostStore {

    private final HashMap<Long, Post> postMap = new HashMap<>(5000); //postID, PostObject
    private final BoundedSortedMultiMap<Integer, Long> postScoreMap = new BoundedSortedMultiMap<>(3, true, true, true);
    private final Long[] previousOrderedTopThree = new Long[3];
    private final StringBuilder builder=new StringBuilder();
    private BufferedWriter writer;

    /**
     *
     * Gets the post list hash map
     *
     * @return the post list
     */
    public HashMap<Long, Post> getPostMap()
    {
        return postMap;
    }

    /**
     * The constructor
     *
     */
    public PostStore(){
        File q1= new File("q1.txt");
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
        postMap.put(postId, post);
        return post;
    }

    /**
     * Get the post object of a given post ID
     *
     * @param postId the post id
     * @return the Post object related to postId
     */
    public Post getPost(Long postId){
        return postMap.get(postId);
    }

    /**
     *
     * Gets map which contains the top three posts in no particular order
     * @return the postScoreMap
     */
    public BoundedSortedMultiMap<Integer, Long> getPostScoreMap()
    {
        return postScoreMap;
    }

    /**
     * Gets the map which contains the top three posts in required order
     *
     * @return the map with top three scores
     */
    private TreeMultimap<Integer, Post>  getTopThreePostsMap() {

        TreeMultimap<Integer, Post> topScoreMap = TreeMultimap.create(Comparator.reverseOrder(), new PostComparator());

        Iterator<Map.Entry<Integer, Long>> iterator = postScoreMap.entrySet().iterator();
        for (; iterator.hasNext(); ) {
            Map.Entry<Integer, Long> entry = iterator.next();
            int score = entry.getKey();
            long id = entry.getValue();
            topScoreMap.put(score, postMap.get(id));
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
     * @param timestamp is the timestamp of event that might trigger a change
     * @param printComments print output
     * @param writeToFile write the output to a file
     * @param delimiter the delimiter to use
     */

    public long printTopThreeComments(Long timestamp, boolean printComments, boolean writeToFile, String delimiter) {

        try{
                builder.setLength(0);
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                String formattedDate = dateFormat.format(new java.util.Date(timestamp));
                builder.append(formattedDate).append(delimiter);
                for (int k = 0; k < 3 ; k++){
                    if (this.previousOrderedTopThree[k] != null){
                        Post post = postMap.get(this.previousOrderedTopThree[k]);
                        builder.append(this.previousOrderedTopThree[k]).append(delimiter);
                        builder.append(postMap.get(this.previousOrderedTopThree[k]).getUserName()).append(delimiter);
                        builder.append(post.getTotalScore()).append(delimiter);
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

