package org.wso2.siddhi.debs2016.sender;

import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2016.util.Constants;
import scala.collection.immutable.Stream;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by anoukh on 3/15/16.
 */
public class OrderedEventSenderThread extends Thread {

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
    public OrderedEventSenderThread(LinkedBlockingQueue<Object[]> eventBuffer [], InputHandler inputHandler [], long eventCount){
        super("Event Sender");
        this.eventBufferList = eventBuffer;
        this.inputHandler = inputHandler;
        this.EVENT_COUNT = eventCount;
    }


    public void run(){
        Object[] friendshipEvent = null;
        Object[] commentEvent = null;
        Object[] likeEvent = null;

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
        int flag = 3;
<<<<<<< HEAD

        while(true){
        try{
            if (flag == Constants.FRIENDSHIPS){
                friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
            }else if (flag == 1){
                commentEvent = eventBufferList[Constants.COMMENTS].take();
            }else if (flag == 2){
                likeEvent = eventBufferList[Constants.LIKES].take();
            }else{
                friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                commentEvent = eventBufferList[Constants.COMMENTS].take();
                likeEvent = eventBufferList[Constants.LIKES].take();
            }
=======
>>>>>>> 5ff111fcd530b2a197b71460310b13f47f2360f1

        while(true){

            try {
                //Send dummy event to mark the commencement of processing
                if(firstEvent){
                    Object[] finalFriendshipEvent = new Object[]{
                            0L,
                            -1L,
                            0L
                    };
                    cTime = System.currentTimeMillis();
                    finalFriendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime;
                    inputHandler[Constants.FRIENDSHIPS].send(cTime, finalFriendshipEvent);

                    //We print the start and the end times of the experiment even if the performance logging is disabled.
                    startDateTime = new Date();
                    startTime = startDateTime.getTime();
                    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
                    System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
                    firstEvent = false;
                }

                try{
                    if (flag == Constants.FRIENDSHIPS){
                        friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].poll(500, TimeUnit.MILLISECONDS);
                    }else if (flag == 1){
                        commentEvent = eventBufferList[Constants.COMMENTS].poll(500, TimeUnit.MILLISECONDS);
                    }else if (flag == 2){
                        likeEvent = eventBufferList[Constants.LIKES].poll(500, TimeUnit.MILLISECONDS);
                    }else{
                        friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                        commentEvent = eventBufferList[Constants.COMMENTS].take();
                        likeEvent = eventBufferList[Constants.LIKES].take();
                    }

                }catch (Exception ex){
                    ex.printStackTrace();
                }
<<<<<<< HEAD

                long tsFriendship;
                long tsComment;
                long tsLike;

                //handling the instance where the stream of a buffer has no more events
                if (friendshipEvent == null){
                    tsFriendship = Long.MAX_VALUE;
                }else{
                    tsFriendship = (Long) friendshipEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (commentEvent == null){
                    tsComment = Long.MAX_VALUE;
                }else{
                    tsComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (likeEvent == null){
                    tsLike = Long.MAX_VALUE;
                }else{
                    tsLike = (Long) likeEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

=======

                long tsFriendship;
                long tsComment;
                long tsLike;

                //handling the instance where the stream of a buffer has no more events
                if (friendshipEvent == null){
                    tsFriendship = Long.MAX_VALUE;
                }else{
                    tsFriendship = (Long) friendshipEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (commentEvent == null){
                    tsComment = Long.MAX_VALUE;
                }else{
                    tsComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (likeEvent == null){
                    tsLike = Long.MAX_VALUE;
                }else{
                    tsLike = (Long) likeEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

>>>>>>> 5ff111fcd530b2a197b71460310b13f47f2360f1
                if (tsFriendship <= tsComment && tsFriendship <= tsLike && tsFriendship != Long.MAX_VALUE){
                    cTime = System.currentTimeMillis();
                    friendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp
                    inputHandler[Constants.FRIENDSHIPS].send(cTime, friendshipEvent);
                    flag = Constants.FRIENDSHIPS;
                }else if (tsComment <= tsFriendship && tsComment <= tsLike && tsComment != Long.MAX_VALUE){
                    cTime = System.currentTimeMillis();
                    commentEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp
                    inputHandler[Constants.COMMENTS].send(cTime, commentEvent);
                    flag = Constants.COMMENTS;
                }else if (tsLike != Long.MAX_VALUE){
                    cTime = System.currentTimeMillis();
                    likeEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp
                    inputHandler[Constants.LIKES].send(cTime, likeEvent);
                    flag = Constants.LIKES;
                }

                count++;

                //When all buffers are empty
                if (friendshipEvent == null && commentEvent == null && likeEvent == null){

//                    Thread.sleep(5000);

//                    friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].poll();
//                    commentEvent = eventBufferList[Constants.COMMENTS].poll();
//                    likeEvent = eventBufferList[Constants.LIKES].poll();

                    //Sending second dummy event to signal end of streams
                    if (friendshipEvent == null && commentEvent == null && likeEvent == null){
                        cTime = System.currentTimeMillis();

                        Object[] finalFriendshipEvent = new Object[]{
                                0L,
                                -2L,
                                0L
                        };

                        finalFriendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime;
                        inputHandler[Constants.FRIENDSHIPS].send(cTime, finalFriendshipEvent);

                        doneFlag = true;
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
