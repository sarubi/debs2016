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

package org.wso2.siddhi.debs2016.comment;

import edu.ucla.sspace.util.BoundedSortedMultiMap;
import org.wso2.siddhi.debs2016.Processors.Q1EventManager;
import org.wso2.siddhi.debs2016.Processors.Q1EventSingle;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class TimeWindow {


    private final LinkedBlockingQueue<TimeWindowComponent> oneDay = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> twoDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> threeDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> fourDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> fiveDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> sixDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> sevenDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> eightDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> nineDays = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<TimeWindowComponent> tenDays = new LinkedBlockingQueue<>();
    private final PostStore postStore;
    private final BoundedSortedMultiMap<Integer, Long> postScoreMap;
    private final CommentPostMap commentPostMap;

    /**
     * The constructor
     * @param postStore the post score object
     * @param commentPostMap the comment post map object
     */
    public TimeWindow(PostStore postStore, CommentPostMap commentPostMap)
    {
        this.postStore = postStore;
        this.postScoreMap = postStore.getPostScoreMap();
        this.commentPostMap = commentPostMap;
    }


    /**
     * Register a new comment in the Time Window
     * @param post is the post object that received the new comment
     * @param ts is the time of arrival of the new comment
     */
    public void addComment(Post post, long ts, long commenterId){
        long postId = post.getPostId();

        oneDay.add(new TimeWindowComponent(post, ts, commenterId, false));
        postScoreMap.remove(post.getTotalScore(), postId);
        post.addComment(ts, commenterId);
        postScoreMap.put(post.getTotalScore(), postId);
    }

    /**
     *
     * Add a new post to the post window
     *
     * @param post the new post
     */
    public void addNewPost(long timestamp, Post post){
        oneDay.add(new TimeWindowComponent(post, timestamp, 0, true));
        postScoreMap.put(10, post.getPostId());
    }

    /**
     * Move the comments and posts along the time axis
     * @param ts time stamp
     */
    public boolean updateTime(long ts){

        process(ts, oneDay, twoDays, 1);
        process(ts, twoDays, threeDays, 2);
        process(ts, threeDays, fourDays, 3);
        process(ts, fourDays, fiveDays, 4);
        process(ts, fiveDays, sixDays, 5);
        process(ts, sixDays, sevenDays, 6);
        process(ts, sevenDays, eightDays, 7);
        process(ts, eightDays, nineDays, 8);
        process(ts, nineDays, tenDays, 9);
        return process(ts, tenDays, null, 10);

    }

    /**
     * Processes a given time window
     *
     * @param ts the new event time
     * @param queue the window iterator
     * @param queueNumber the window number
     */
    private boolean process(long ts, LinkedBlockingQueue<TimeWindowComponent> queue, LinkedBlockingQueue<TimeWindowComponent> nextQueue, int queueNumber) {
        try {
            HashMap<Long, Post> postMap = postStore.getPostMap();
            Iterator<TimeWindowComponent> iterator = queue.iterator();

            while (iterator.hasNext()) {
                TimeWindowComponent timeWindowComponent = iterator.next();
                long objectArrivalTime = timeWindowComponent.getTimestamp();
                if (objectArrivalTime <= (ts - (CommentPostMap.DURATION * queueNumber))) {
                    Post post = timeWindowComponent.getPost();
                    long postID = post.getPostId();
                    int oldScore = post.getTotalScore();
                    postScoreMap.remove(oldScore, postID);
                    post.decrementTotalScore();
                    int newScore = post.getTotalScore();
                    if (postStore.getPostMap().containsKey(postID)) {
                        boolean isPost = timeWindowComponent.isPost();
                        timeWindowComponent.setExpiringTime(timeWindowComponent.getExpiringTime() + CommentPostMap.DURATION);
                        if (newScore <= 0) {
                            postMap.remove(postID);
                            commentPostMap.getCommentToPostMap().remove(postID);
                            Q1EventSingle.timeOfEvent = timeWindowComponent.getExpiringTime();
                        } else {
                            postScoreMap.put(newScore, postID);
                            if (nextQueue != null) {
                                nextQueue.add(timeWindowComponent);
                            } else {
                                if (!isPost) {
                                    post.removeCommenter(timeWindowComponent.getUserId());
                                }
                            }
                        }
                    }
                    iterator.remove();
                } else {
                    break;
                }
            }
            return nextQueue == null && postStore.hasTopThreeChanged();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
