package org.wso2.siddhi.debs2016.sender;

import org.wso2.siddhi.debs2016.Processors.DEBSEvent;
import org.wso2.siddhi.debs2016.Processors.Q1EventManager;
import org.wso2.siddhi.debs2016.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * The event sender for query 1
 *
 */
public class OrderedEventSenderThreadQ1 extends Thread {

    private final LinkedBlockingQueue<Object[]>[] eventBufferList;
    private final Q1EventManager manager = new Q1EventManager();

    /**
     * The constructor
     *
     * @param eventBuffer  the event buffer array
     */
    public OrderedEventSenderThreadQ1(LinkedBlockingQueue<Object[]> eventBuffer[]) {
        super("Event Sender Query 1");
        this.eventBufferList = eventBuffer;
        manager.run();
    }


    public void run() {
        Object[] commentEvent = null;
        Object[] postEvent = null;
        long cTime ;
        boolean firstEvent = true;
        int flag = Constants.NOEVENT;
        boolean postLastEventArrived = false;
        boolean commentsLastEventArrived = false;
        while (true) {

            try {
                if (firstEvent) {
                    Object[] firstPostEvent = new Object[]{
                            0L,
                            -1L,
                            0L,
                            0L,
                            "",
                            "",
                            0L,
                            0L,
                            Constants.POSTS
                    };
                    cTime = System.currentTimeMillis();
                    DEBSEvent event = manager.getNextDebsEvent();
                    event.setObjectArray(firstPostEvent);
                    event.setSystemArrivalTime(cTime);
                    manager.publish();
                    //We print the start and the end times of the experiment even if the performance logging is disabled.
                    firstEvent = false;
                }

                try {

                    if (flag == Constants.POSTS) {
                        if(!postLastEventArrived) {
                            postEvent = eventBufferList[Constants.POSTS].take();
                            long lastEvent = (Long) postEvent[0];
                            if (lastEvent == -1L)
                            {
                                postEvent = null;
                                postLastEventArrived = true;
                            }
                        }
                    } else if (flag == Constants.COMMENTS) {
                        if(!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L)
                            {
                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }
                        }
                    } else {
                        if(!postLastEventArrived) {
                            postEvent = eventBufferList[Constants.POSTS].take();
                            long lastEvent = (Long) postEvent[0];
                            if (lastEvent == -1L)
                            {
                                postEvent = null;
                                postLastEventArrived = true;
                            }
                        }
                        if(!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L)
                            {
                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }
                        }
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                long tsComment;
                long tsPost;

                if (commentEvent == null) {
                    tsComment = Long.MAX_VALUE;
                } else {
                    tsComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (postEvent == null) {
                    tsPost = Long.MAX_VALUE;
                } else {
                    tsPost = (Long) postEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (tsComment < tsPost && tsComment != Long.MAX_VALUE) {

                    cTime = System.currentTimeMillis();
                    DEBSEvent event = manager.getNextDebsEvent();
                    event.setObjectArray(commentEvent);
                    event.setSystemArrivalTime(cTime);
                    manager.publish();
                    flag = Constants.COMMENTS;
                } else if (tsPost != Long.MAX_VALUE) {

                    cTime = System.currentTimeMillis();
                    DEBSEvent event = manager.getNextDebsEvent();
                    event.setObjectArray(postEvent);
                    event.setSystemArrivalTime(cTime);
                     manager.publish();
                    flag = Constants.POSTS;
                }

                //When all buffers are empty
                if (commentEvent == null && postEvent == null) {
                    //Sending second dummy event to signal end of streams
                    Object[] finalPostEvent = new Object[]{
                            0L,
                            -2L,
                            0L,
                            0L,
                            "",
                            "",
                            0L,
                            0L,
                            Constants.POSTS
                    };
                    cTime = System.currentTimeMillis();
                    DEBSEvent event = manager.getNextDebsEvent();
                    event.setObjectArray(finalPostEvent);
                    event.setSystemArrivalTime(cTime);
                    manager.publish();
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}