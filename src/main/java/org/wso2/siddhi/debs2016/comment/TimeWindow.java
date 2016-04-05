package org.wso2.siddhi.debs2016.comment;

import edu.ucla.sspace.util.BoundedSortedMultiMap;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;
import org.wso2.siddhi.debs2016.post.PostWindowObject;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by anoukh on 3/29/16.
 */
public class TimeWindow {


    LinkedBlockingQueue<CommentForPost> oneDay = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> twoDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> threeDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> fourDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> fiveDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> sixDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> sevenDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> eightDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> nineDays = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<CommentForPost> tenDays = new LinkedBlockingQueue<>();
    private PostStore postStore;
    BoundedSortedMultiMap<Integer, Long> postScoreMap;

    LinkedList<PostWindowObject> postWindow = new LinkedList<>();

    /**
     * The constructor
     * @param postStore the post score object
     */
    public TimeWindow(PostStore postStore)
    {
        this.postStore = postStore;
        this.postScoreMap = postStore.getPostScoreMap();
    }


    /**
     * Register a new comment in the Time Window
     * @param post is the post object that received the new comment
     * @param ts is the time of arrival of the new comment
     */
    public void addComment(Post post, long ts, long commenter_id){
        long postId = post.getPostId();
        oneDay.add(new CommentForPost(post, ts));
        postScoreMap.remove(post.getTotalScore(), postId);
        post.addComment(ts, commenter_id);
        postScoreMap.put(post.getTotalScore(), postId);
    }

    /**
     *
     * Add a new post with no comments
     *
     * @param post the new post
     */
    public void addNewPost(long ts, Post post){
        postWindow.addFirst(new PostWindowObject(ts, post));
        postScoreMap.put(10, post.getPostId());
    }

    /**
     * Move the comments along the time axis
     * @param ts time stamp
     */
    public void updateTime(long ts){

        process(ts, oneDay, twoDays, 1);
        process(ts, twoDays, threeDays, 2);
        process(ts, threeDays, fourDays, 3);
        process(ts, fourDays, fiveDays, 4);
        process(ts, fiveDays, sixDays, 5);
        process(ts, sixDays, sevenDays, 6);
        process(ts, sevenDays, eightDays, 7);
        process(ts, eightDays, nineDays, 8);
        process(ts, nineDays, tenDays, 9);
        process(ts, tenDays, null, 10);
        processPost(ts);

    }

    /**
     * Processes a given time window
     *
     * @param ts the new event time
     * @param queue the window iterator
     * @param queueNumber the window number
     */
    private void process(long ts,  LinkedBlockingQueue<CommentForPost> queue, LinkedBlockingQueue<CommentForPost> nextQueue, int queueNumber) {

        try {

            Iterator<CommentForPost> iterator = queue.iterator();

            while (iterator.hasNext()) { //Iterate over Queue
                CommentForPost commentPostObject = iterator.next();
                long commentTs = commentPostObject.getTs();
                if (commentTs <= (ts - CommentPostMap.DURATION * queueNumber)) {
                    Post post = commentPostObject.getPost();
                    long postID = post.getPostId();
                    int oldScore = post.getTotalScore();
                    postScoreMap.remove(oldScore, postID);
                    post.decrementTotalScore();
                    int newScore = post.getTotalScore();
                    postScoreMap.put(newScore, postID);
                    if (nextQueue != null) {
                        nextQueue.add(commentPostObject);
                    }
                    iterator.remove();
                } else {
                    break;
                }
            }
        } catch (java.lang.Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     *
     * Process the post
     *
     * @param ts the event time
     */
    private void processPost(long ts){

        Iterator<PostWindowObject> iterator = postWindow.descendingIterator();
        HashMap<Long, Post> postMap = postStore.getPostList();
        ArrayList<PostWindowObject> deductedPosts = new ArrayList<>();

        while (iterator.hasNext()){
            PostWindowObject postObject = iterator.next();
            Post post = postObject.getPost();
            long postId = post.getPostId();
            int oldScore = post.getTotalScore();
            long postArrivalTime = postObject.getArrivalTime();


            //Check how many days it has passed
            while (postArrivalTime <= (ts - CommentPostMap.DURATION)){
                post.decrementTotalScore();
                postArrivalTime = postArrivalTime + CommentPostMap.DURATION;
            }
            int newScore = post.getTotalScore();
            if (oldScore != newScore){
                iterator.remove();
                postScoreMap.remove(oldScore, postId);
                if(newScore <= 0){
                    postMap.remove(postId);
                }else{
                    deductedPosts.add(new PostWindowObject(postArrivalTime, post));
                    postScoreMap.put(post.getTotalScore(), postId);
                }
            }
        }

        for (PostWindowObject postObject:
             deductedPosts) {
            postWindow.addFirst(postObject);
        }
    }
}
