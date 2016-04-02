package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;

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

    /**
     * Register a new comment in the Time Window
     * @param post is the post object that received the new comment
     * @param ts is the time of arrival of the new comment
     */
    public void addComment(Post post, long ts){
        oneDay.add(new CommentForPost(post, ts));
        updateTime(ts);
    }

    /**
     * Move the comments along the time axis
     * @param ts time stamp
     */
    public void updateTime(long ts){

        process(ts, oneDay.iterator(), 1);
        process(ts, twoDays.iterator(), 2);
        process(ts, threeDays.iterator(), 3);
        process(ts, fourDays.iterator(), 4);
        process(ts, fiveDays.iterator(), 5);
        process(ts, sixDays.iterator(), 6);
        process(ts, sevenDays.iterator(), 7);
        process(ts, eightDays.iterator(), 8);
        process(ts, nineDays.iterator(), 9);
        process(ts, tenDays.iterator(), 10);

    }



    /**
     * Processes a given time window
     *
     * @param ts the new event time
     * @param iterator the window iterator
     * @param queueNumber the window number
     */
    private void process(long ts, Iterator<CommentForPost> iterator, int queueNumber) {

        while (iterator.hasNext()){
            CommentForPost commentPostObject = iterator.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*queueNumber)){
                commentPostObject.getPost().decrementScore();
                iterator.remove();
            }else{
                break;
            }
        }
    }
}
/**
 * An Object to record the timestamp of the comment of a post
 */
class CommentForPost{
    private long ts;

    public Post getPost() {
        return post;
    }

    public long getTs() {
        return ts;
    }

    private Post post;

    public CommentForPost(Post post, long ts) {
        this.ts = ts;
        this.post = post;
    }
}
