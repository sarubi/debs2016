package org.wso2.siddhi.debs2016.sender;

/**
 * Created by malithjayasinghe on 4/6/16.
 */

import org.wso2.siddhi.debs2016.Processors.DEBSEvent;
import org.wso2.siddhi.debs2016.Processors.Q2EventManager;
import org.wso2.siddhi.debs2016.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The event sender for query 2
 *
 * Created by anoukh on 3/15/16.
 */
public class OrderedEventSenderThreadQ2 extends Thread {

    private LinkedBlockingQueue<Object[]> eventBufferList [];
    private Date startDateTime;
    public boolean doneFlag = false;

    Q2EventManager manager = new Q2EventManager();


    /**
     * The constructor
     *
     * @param eventBuffer the event buffer array
     */
    public OrderedEventSenderThreadQ2(LinkedBlockingQueue<Object[]> eventBuffer []) {
        // super("Event Sender");
        this.eventBufferList = eventBuffer;
        //this.inputHandler = inputHandler;
        manager.run();
    }

    /**
     *
     * Start the data reader thread
     *
     */
    public void run(){
        Object[] friendshipEvent = null;
        Object[] commentEvent = null;
        Object[] likeEvent = null;
        long startTime = 0;
        long cTime = 0;
        boolean firstEvent = true;
        int flag = Constants.NOEVENT;
        int count=0;

        while(true){
            //Send dummy event to mark the commencement of processing
            if(firstEvent){
                Object[] finalFriendshipEvent = new Object[]{
                        0L,
                        -1L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0,
                };
                cTime = System.currentTimeMillis();
                DEBSEvent event = manager.getNextDebsEvent();
                event.setHandlerId(-1);
                event.setObjectArray(finalFriendshipEvent);
                event.setSystemArrivalTime(cTime);
                manager.publish();

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

            if (tsFriendship <= tsComment && tsFriendship <= tsLike && tsFriendship != Long.MAX_VALUE){
                cTime = System.currentTimeMillis();
                DEBSEvent debsEvent = manager.getNextDebsEvent();
                debsEvent.setObjectArray(friendshipEvent);
                debsEvent.setSystemArrivalTime(cTime);
                debsEvent.setHandlerId(-1);
                manager.publish();
                flag = Constants.FRIENDSHIPS;
            }else if (tsComment <= tsFriendship && tsComment <= tsLike && tsComment != Long.MAX_VALUE){
                cTime = System.currentTimeMillis();
                DEBSEvent debsEvent = manager.getNextDebsEvent();
                debsEvent.setObjectArray(commentEvent);
                debsEvent.setSystemArrivalTime(cTime);
                debsEvent.setHandlerId((long)(commentEvent[3])%4);
                manager.publish();
                flag = Constants.COMMENTS;
            }else if (tsLike != Long.MAX_VALUE){
                cTime = System.currentTimeMillis();
                DEBSEvent debsEvent = manager.getNextDebsEvent();
                debsEvent.setObjectArray(likeEvent);
                debsEvent.setSystemArrivalTime(cTime);
                debsEvent.setHandlerId((long)(likeEvent[3])%4);
                manager.publish();
                flag = Constants.LIKES;
            }

            //When all buffers are empty
            if (friendshipEvent == null && commentEvent == null && likeEvent == null){
                cTime = System.currentTimeMillis();
                //Send dummy event to signal end of all streams
                Object[] finalFriendshipEvent = new Object[]{
                        0L,
                        -2L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0L,
                        0,
                };

                DEBSEvent debsEvent = manager.getNextDebsEvent();
                debsEvent.setObjectArray(finalFriendshipEvent);
                debsEvent.setSystemArrivalTime(cTime);
                debsEvent.setHandlerId(-1);
                manager.publish();
                manager.getDataReadDisruptor().shutdown();
                doneFlag = true;
                break;
            }
        }

    }
}