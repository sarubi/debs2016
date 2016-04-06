package org.wso2.siddhi.debs2016.sender;

import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2016.Processors.DEBSEvent;
import org.wso2.siddhi.debs2016.Processors.Q2EventManager;
import org.wso2.siddhi.debs2016.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * The event sender for query 1
 *
 */
public class OrderedEventSenderThreadQ1 extends Thread {

    private LinkedBlockingQueue<Object[]> eventBufferList[];
    private Date startDateTime;
    public boolean doneFlag = false;
    Q2EventManager manager = new Q2EventManager();

    /**
     * The constructor
     *
     * @param eventBuffer  the event buffer array
     * @param inputHandler the input handler array
     */
    public OrderedEventSenderThreadQ1(LinkedBlockingQueue<Object[]> eventBuffer[], InputHandler inputHandler) {
        super("Event Sender");
        this.eventBufferList = eventBuffer;
    }


    public void run() {
        Object[] commentEvent = null;
        Object[] postEvent = null;

        long count = 1;
        long timeDifferenceFromStart = 0;
        long timeDifference = 0; //This is the time difference for this time window.
        long currentTime = 0;
        long prevTime = 0;
        //long startTime = System.currentTimeMillis();
        long startTime = 0;
        long cTime = 0;
        //Special note : Originally we need not subtract 1. However, due to some reason if there are n events in the input data set that are
        //pumped to the eventBufferList queue, only (n-1) is read. Therefore, we have -1 here.
        //final int EVENT_COUNT = Integer.parseInt(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset.size")) - 1;

        boolean firstEvent = true;
        float percentageCompleted = 0;
        int flag = Constants.NOEVENT;

        while (true) {

            try {
                //Send dummy event to mark the commencement of processing
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
                    firstPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = cTime;


                    cTime = System.currentTimeMillis();
                    DEBSEvent event = manager.getNextDebsEvent();
                    event.setObjectArray(firstPostEvent);
                    event.setSystemArrivalTime(cTime);
                    manager.publish();
                    //We print the start and the end times of the experiment even if the performance logging is disabled.
                    startDateTime = new Date();
                    startTime = startDateTime.getTime();
                    SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
                    System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
                    firstEvent = false;
                }

                try {

                    if (flag == Constants.POSTS) {
                        postEvent = eventBufferList[Constants.POSTS].poll(1000, TimeUnit.MILLISECONDS);
                    } else if (flag == Constants.COMMENTS) {
                        commentEvent = eventBufferList[Constants.COMMENTS].poll(1000, TimeUnit.MILLISECONDS);
                    } else {
                        postEvent = eventBufferList[Constants.POSTS].poll(1000, TimeUnit.MILLISECONDS);
                        commentEvent = eventBufferList[Constants.COMMENTS].poll(1000, TimeUnit.MILLISECONDS);
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                long tsComment;
                long tsPost;

                //handling the instance where the stream of a buffer has no more events
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
                    commentEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = cTime; //This corresponds to the iij_timestamp
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
                    manager.publish();;
                    flag = Constants.POSTS;
                }

                count++;

                //When all buffers are empty
                if (commentEvent == null && postEvent == null) {
                    //Sending second dummy event to signal end of streams
                    cTime = System.currentTimeMillis();

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

                    finalPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = cTime;
                    Thread.sleep(1000);//We just sleep for short period so that we can ensure that all the data events have been processed by the ranker properly before we shutdown.
                    cTime = System.currentTimeMillis();
                    DEBSEvent event = manager.getNextDebsEvent();
                    event.setObjectArray(finalPostEvent);
                    event.setSystemArrivalTime(cTime);
                    manager.publish();
                    doneFlag = true;
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}