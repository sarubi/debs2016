package org.wso2.siddhi.debs2016.comment;

import edu.ucla.sspace.util.BoundedSortedMultiMap;
import edu.ucla.sspace.util.SortedMultiMap;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;
import scala.util.control.Exception;

import java.util.HashMap;
import java.util.Iterator;
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

    /**
     * The constructor
     * @param postStore the post score object
     */
    public TimeWindow(PostStore postStore)
    {
        this.postStore = postStore;
    }


    /**
     * Register a new comment in the Time Window
     * @param post is the post object that received the new comment
     * @param ts is the time of arrival of the new comment
     */
    public void addComment(Post post, long ts){
        this.postStore = postStore;
        oneDay.add(new CommentForPost(post, ts));
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

            BoundedSortedMultiMap<Long, Long> postScoreMap = postStore.getPostScoreMap();
            Iterator<CommentForPost> iterator = queue.iterator();
            HashMap<Long, Post> postMap = postStore.getPostList();

            while (iterator.hasNext()) {
                CommentForPost commentPostObject = iterator.next();
                long commentTs = commentPostObject.getTs();
                if (commentTs <= (ts - CommentPostMap.DURATION * queueNumber)) {
                    Post post = commentPostObject.getPost();
                    long postID = post.getPostId();
                    long oldPostScore = post.getScore(ts);

                    postScoreMap.remove(oldPostScore, postID);
                    if (nextQueue != null) {
                        nextQueue.add(commentPostObject);
                    }

                    long newScore = post.decrementScore();

                    if(newScore > 0) {
                        postScoreMap.put(newScore, postID);
                    }

                    iterator.remove();

                    if(newScore < 0)
                    {
                        postMap.remove(postID);
                    }

                } else {
                    break;
                }
            }
        } catch (java.lang.Exception e)
        {
            e.printStackTrace();
        }
    }


}
