package org.wso2.siddhi.debs2016.sender;

import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2016.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by anoukh on 3/15/16.
 */
public class OrderedEventSenderThreadQuery1 extends Thread {

    private LinkedBlockingQueue<Object[]> eventBufferList [];
    private InputHandler inputHandler [];
    private Date startDateTime;
    private long EVENT_COUNT;
    public boolean doneFlag = false;


    /**
     * The constructor
     *
     * @param eventBuffer the event buffer array
     * @param inputHandler the input handler array
     * @param eventCount the event count
     */
    public OrderedEventSenderThreadQuery1(LinkedBlockingQueue<Object[]> eventBuffer[], InputHandler inputHandler[], long eventCount){
        super("Event Sender");
        this.eventBufferList = eventBuffer;
        this.inputHandler = inputHandler;
        this.EVENT_COUNT = eventCount;
    }


    public void run(){
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

        while(true){

            try {
                //Send dummy event to mark the commencement of processing
                if(firstEvent){
                    Object[] finalPostEvent = new Object[]{
                            0L,
                            -1L,
                            0L,
                            0L,
                            "",
                            ""
                    };
                    cTime = System.currentTimeMillis();
                    finalPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime;
                    inputHandler[Constants.POSTS].send(cTime, finalPostEvent);

                    //We print the start and the end times of the experiment even if the performance logging is disabled.
                    startDateTime = new Date();
                    startTime = startDateTime.getTime();
                    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
                    System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
                    firstEvent = false;
                }

                try{
//                    if (flag == Constants.COMMENTS){
//                        commentEvent = eventBufferList[Constants.COMMENTS].poll(500, TimeUnit.MILLISECONDS);
//                    }else if (flag == Constants.POSTS){
//                        postEvent = eventBufferList[Constants.POSTS].poll(500, TimeUnit.MILLISECONDS);
//                    }else{
//                        commentEvent = eventBufferList[Constants.COMMENTS].take();
//                        postEvent = eventBufferList[Constants.POSTS].take();
//                    }

                    if (flag == Constants.POSTS){
                        postEvent = eventBufferList[Constants.POSTS].poll(500, TimeUnit.MILLISECONDS);
                    }else if (flag == Constants.COMMENTS){
                        commentEvent = eventBufferList[Constants.COMMENTS].poll(500, TimeUnit.MILLISECONDS);
                    }else{
                        commentEvent = eventBufferList[Constants.COMMENTS].take();
                        postEvent = eventBufferList[Constants.POSTS].take();
                    }

                }catch (Exception ex){
                    ex.printStackTrace();
                }

                long tsFriendship;
                long tsComment;
                long tsLike;
                long tsPost;

                //handling the instance where the stream of a buffer has no more events
                if (commentEvent == null){
                    tsComment = Long.MAX_VALUE;
                }else{
                    tsComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                    System.out.println("------>cc");
                }

                if (postEvent == null){
                    tsPost = Long.MAX_VALUE;
                }else{
                    tsPost = (Long) postEvent[Constants.EVENT_TIMESTAMP_FIELD];
                    System.out.println("------>pp");
                }

                if (tsComment < tsPost && tsComment != Long.MAX_VALUE){
                    cTime = System.currentTimeMillis();
                    commentEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp
                    inputHandler[Constants.COMMENTS].send(cTime, commentEvent);
                    flag = Constants.COMMENTS;
                    System.out.println("------>c");
                }else if (tsPost != Long.MAX_VALUE){
                    System.out.println("------>p");
                    cTime = System.currentTimeMillis();
                    postEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp
                    inputHandler[Constants.POSTS].send(cTime, postEvent);
                    flag = Constants.POSTS;
                }

                count++;

                //When all buffers are empty
                if (commentEvent == null && postEvent == null){
                    //Sending second dummy event to signal end of streams
                        cTime = System.currentTimeMillis();

                        Object[] finalPostEvent = new Object[]{
                                0L,
                                -2L,
                                0L,
                                0L,
                                "",
                                ""
                        };

                        finalPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime;
                        Thread.sleep(1000);//We just sleep for short period so that we can ensure that all the data events have been processed by the ranker properly before we shutdown.
                        inputHandler[Constants.POSTS].send(cTime, finalPostEvent);

                        doneFlag = true;
                        break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}