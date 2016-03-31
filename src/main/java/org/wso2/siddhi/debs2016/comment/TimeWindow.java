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
     * @param ts
     */
    public void updateTime(long ts){

        Iterator<CommentForPost> iteratorOne = oneDay.iterator();
        while (iteratorOne.hasNext()){
            CommentForPost commentPostObject = iteratorOne.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION)){
                commentPostObject.getPost().decrementScore();
                twoDays.add(commentPostObject);
                iteratorOne.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorTwo = twoDays.iterator();
        while (iteratorTwo.hasNext()){
            CommentForPost commentPostObject = iteratorTwo.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*2)){
                commentPostObject.getPost().decrementScore();
                threeDays.add(commentPostObject);
                iteratorTwo.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorThree = threeDays.iterator();
        while (iteratorThree.hasNext()){
            CommentForPost commentPostObject = iteratorThree.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*3)){
                commentPostObject.getPost().decrementScore();
                fourDays.add(commentPostObject);
                iteratorThree.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorFour = fourDays.iterator();
        while (iteratorFour.hasNext()){
            CommentForPost commentPostObject = iteratorFour.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*4)){
                commentPostObject.getPost().decrementScore();
                fiveDays.add(commentPostObject);
                iteratorFour.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorFive = fiveDays.iterator();
        while (iteratorFive.hasNext()){
            CommentForPost commentPostObject = iteratorFive.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*5)){
                commentPostObject.getPost().decrementScore();
                sixDays.add(commentPostObject);
                iteratorFive.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorSix = sixDays.iterator();
        while (iteratorSix.hasNext()){
            CommentForPost commentPostObject = iteratorSix.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*6)){
                commentPostObject.getPost().decrementScore();
                sevenDays.add(commentPostObject);
                iteratorSix.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorSeven = sevenDays.iterator();
        while (iteratorSeven.hasNext()){
            CommentForPost commentPostObject = iteratorSeven.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*7)){
                commentPostObject.getPost().decrementScore();
                eightDays.add(commentPostObject);
                iteratorSeven.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorEight = eightDays.iterator();
        while (iteratorEight.hasNext()){
            CommentForPost commentPostObject = iteratorEight.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*8)){
                commentPostObject.getPost().decrementScore();
                nineDays.add(commentPostObject);
                iteratorEight.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorNine = nineDays.iterator();
        while (iteratorNine.hasNext()){
            CommentForPost commentPostObject = iteratorNine.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*9)){
                commentPostObject.getPost().decrementScore();
                tenDays.add(commentPostObject);
                iteratorNine.remove();
            }else{
                break;
            }
        }

        Iterator<CommentForPost> iteratorTen = tenDays.iterator();
        while (iteratorTen.hasNext()){
            CommentForPost commentPostObject = iteratorTen.next();
            long commentTs = commentPostObject.getTs();
            if (commentTs <= (ts - CommentPostMap.DURATION*10)){
                commentPostObject.getPost().decrementScore();
                iteratorTen.remove();
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
