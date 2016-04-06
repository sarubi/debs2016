package org.wso2.siddhi.debs2016.Processors;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.comment.TimeWindow;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.Post;
import org.wso2.siddhi.debs2016.post.PostStore;
import org.wso2.siddhi.debs2016.util.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created by malithjayasinghe on 4/6/16.
 */
public class Q1EventManager {

        private Disruptor<DEBSEvent> dataReadDisruptor;
        private RingBuffer dataReadBuffer;
        private long startiij_timestamp;
        private long endiij_timestamp;
        private String ts;
        long timeDifference = 0; //This is the time difference for this time window.
        static int bufferSize = 512;
        private long sequenceNumber;
        private long count;
        private Date startDateTime;
        long startTime = 0;
        private PostStore postStore;
        private CommentPostMap commentPostMap;
        private TimeWindow timeWindow;
        private Long latency = 0L;
        private Long numberOfOutputs = 0L;

        /**
         * The constructor
         *
         */
        public Q1EventManager(){

            startDateTime = new Date();
            startTime = startDateTime.getTime();
            SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
            System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
            postStore = new PostStore();
            commentPostMap = new CommentPostMap();
            timeWindow = new TimeWindow(postStore);
            System.out.println("Query 1: version 5");
        }

        /**
         *
         * Starts the distrupter
         *
         */
        public void run() {
            dataReadDisruptor = new Disruptor<DEBSEvent>(new com.lmax.disruptor.EventFactory<DEBSEvent>() {

                @Override
                public DEBSEvent newInstance() {
                    return new DEBSEvent();
                }
            }, bufferSize, Executors.newFixedThreadPool(3), ProducerType.SINGLE, new SleepingWaitStrategy());

            //******************Handler**************************************//

            DEBSEventHandler debsEventHandler = new DEBSEventHandler();
            dataReadDisruptor.handleEventsWith(debsEventHandler);
            dataReadBuffer = dataReadDisruptor.start();
        }

        /**
         * Gets the reference to next DebsEvent from the ring butter
         *
         * @return the DebsEvent
         */
        public DEBSEvent getNextDebsEvent()
        {
            sequenceNumber = dataReadBuffer.next();
            return dataReadDisruptor.get(sequenceNumber);
        }


        /**
         * Publish the new event
         *
         */
        public void publish()
        {
            dataReadBuffer.publish(sequenceNumber);
        }


        /**
         *
         * The debs event handler
         *
         */
        private class DEBSEventHandler implements EventHandler<DEBSEvent> {
            @Override
            public void onEvent(DEBSEvent debsEvent, long l, boolean b) throws Exception {
                Object [] objects = debsEvent.getObjectArray();

              /*Answer: When processing a new input tuple, processing steps should be performed in this order:
                1) decrease the score of all previous posts (given the semantics of the query)
                2) increase score of post related to the input tuple,
                3) decrease score of posts expiring precisely on this timestamp, if any,
                4) discard posts with 0 score. Such a post would, thus, survive the transient state. However, a post whose score reached 0 at a timestamp earlier than the current input tuple, will not survive, even if the processing of that timeout happens to be triggered by the current input tuple.
             */
                // Does it make difference in the final result whether we perform 1) 2) 3) in order this order or not?
                // What is the plan for commentPostMap?
                // What aren't we doing timeWindow(ts) at the start?

                //The order given necessarily means that we have to delete a post after updating the store
                //We need commentPostMap to point a comment to comment to the correct post (Eliminates recursion)
                try {
                    long iij_timestamp = (Long) objects[0];
                    endiij_timestamp = iij_timestamp;
                    long ts = (Long) objects[1];
                    String user_name = (String) objects[5];
                    int isPostFlag = (int) objects[8];

                    if (ts == -1L) {
                        //This is the place where time measuring starts.
                        startiij_timestamp = iij_timestamp;

                    }

                    if (ts == -2L) {
                        //This is the place where time measuring ends.
                        showFinalStatistics();
                        postStore.destroy();


                    }
                    count++;
                    Post post;
                    switch (isPostFlag){
                        case Constants.POSTS:
                            long post_id = (Long) objects[2];
                            post = postStore.addPost(post_id, ts, user_name); // 2)
                            timeWindow.updateTime(ts);
                            timeWindow.addNewPost(ts, post);
                            break;

                        case Constants.COMMENTS:
                            long comment_id = (Long) objects[3];
                            long comment_replied_id = (Long) objects[6];
                            long post_replied_id = (Long) objects[7];
                            long commenter_id = (Long) objects[2];

                            if (post_replied_id != -1 && comment_replied_id == -1){
                                post = postStore.getPost(post_replied_id);
                                if (post != null) {
                                    timeWindow.addComment(post, ts, commenter_id);
                                }
                                timeWindow.updateTime(ts);
                                commentPostMap.addCommentToPost(comment_id, post_replied_id);

                            } else if (comment_replied_id != -1 && post_replied_id == -1){
                                long parent_post_id = commentPostMap.addCommentToComment(comment_id, comment_replied_id);
                                post = postStore.getPost(parent_post_id);
                                if (post != null) {
                                    timeWindow.addComment(post, ts, commenter_id);
                                }
                                timeWindow.updateTime(ts);
                            }
                            break;
                    }

                    Long endTime = postStore.printTopThreeComments(ts, true, true, ",");

                    if (endTime != -1L){
                        latency += (endTime - (Long) objects[0]);
                        numberOfOutputs++;
                    }

                    } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }


    /**
     *
     * Print the throughput etc
     *
     */
    private void showFinalStatistics()
    {
        timeDifference = endiij_timestamp - startiij_timestamp;

        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
        System.out.println("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
        System.out.println("Event count : " + count);
        System.out.println("Total run time : " + timeDifference);
        System.out.println("Throughput (events/s): " + Math.round((count * 1000.0) / timeDifference));
        System.out.println("Total Latency " + latency);
        System.out.println("Total Outputs " + numberOfOutputs);
        if (numberOfOutputs!=0){
            System.out.println("Average Latency " + (double)latency/numberOfOutputs);
        }
        System.out.flush();
    }

}


