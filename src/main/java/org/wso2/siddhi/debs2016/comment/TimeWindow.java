package org.wso2.siddhi.debs2016.comment;

import edu.ucla.sspace.util.BoundedSortedMultiMap;
import org.wso2.siddhi.debs2016.Processors.Q1EventManager;
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


    private LinkedBlockingQueue<CommentForPost> oneDay = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> twoDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> threeDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> fourDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> fiveDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> sixDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> sevenDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> eightDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> nineDays = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<CommentForPost> tenDays = new LinkedBlockingQueue<>();
    private PostStore postStore;
    BoundedSortedMultiMap<Integer, Long> postScoreMap;
    CommentPostMap commentPostMap;

    LinkedList<PostWindowObject> postWindow = new LinkedList<>();

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
    public void addComment(Post post, long ts, long commenter_id){
        long postId = post.getPostId();
        oneDay.add(new CommentForPost(post, ts));
        postScoreMap.remove(post.getTotalScore(), postId);
        post.addComment(ts, commenter_id);
        postScoreMap.put(post.getTotalScore(), postId);
    }

    /**
     *
     * Add a new post to the post window
     *
     * @param post the new post
     */
    public void addNewPost(long ts, Post post){
        postWindow.addFirst(new PostWindowObject(ts, post));
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
        process(ts, tenDays, null, 10);
        return processPost(ts);

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
                    if (postStore.getPostList().containsKey(postID)){
                        postScoreMap.put(newScore, postID);
                        if (nextQueue != null) {
                            nextQueue.add(commentPostObject);
                        }
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
     * Process the post window
     *
     * @param ts the event time
     */
    private boolean processPost(long ts){

        Iterator<PostWindowObject> iterator = postWindow.descendingIterator();
        HashMap<Long, Post> postMap = postStore.getPostList();
        ArrayList<PostWindowObject> deductedPosts = new ArrayList<>();
        while (iterator.hasNext()){
            PostWindowObject postObject = iterator.next();
            Post post = postObject.getPost();
            long postId = post.getPostId();
            int oldScore = post.getTotalScore();
            long postArrivalTime = postObject.getArrivalTime();

            long timeWindowStart = ts - CommentPostMap.DURATION;
            if (postArrivalTime > timeWindowStart){
                break;
            }

            //Check how many days it has passed
            while (postArrivalTime <= timeWindowStart){
                postArrivalTime = postArrivalTime + CommentPostMap.DURATION;
                post.decrementTotalScore();
                if (post.getTotalScore() <= 0){
                    postMap.remove(postId);
                    commentPostMap.getCommentToPostMap().remove(postId);
                    Q1EventManager.timeOfEvent = postArrivalTime;
                    break;
                }
            }
            int newScore = post.getTotalScore();
            iterator.remove();
            postScoreMap.remove(oldScore, postId);

            if(newScore > 0){
                deductedPosts.add(new PostWindowObject(postArrivalTime, post));
                postScoreMap.put(post.getTotalScore(), postId);
            }
        }
        addToList(deductedPosts);
        return postStore.hasTopThreeChanged();
    }

    /**
     * Adds the post to the list
     *
     * @param deductedPosts is list of post to add back to the array list
     */
    private void addToList(ArrayList<PostWindowObject> deductedPosts){
        Collections.reverse(deductedPosts);
        Iterator<PostWindowObject> iterator1 = deductedPosts.iterator();
        while (iterator1.hasNext()){
            postWindow.addFirst(iterator1.next());
        }
    }
}
