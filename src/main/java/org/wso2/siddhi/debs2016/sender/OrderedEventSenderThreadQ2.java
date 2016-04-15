package org.wso2.siddhi.debs2016.sender;

/**
 * Created by malithjayasinghe on 4/6/16.
 */

import org.wso2.siddhi.core.stream.input.InputHandler;
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
    private Q2EventManager manager;


    /**
     * The constructor
     *
     * @param eventBuffer the event buffer array
     */
    public OrderedEventSenderThreadQ2(LinkedBlockingQueue<Object[]> eventBuffer [], int k, long d) {
        super("Event Sender");
        this.eventBufferList = eventBuffer;
        manager = new Q2EventManager(k, d*1000);
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
        boolean friendshipLastEventArrived = false;
        boolean commentsLastEventArrived = false;
        boolean likesLastEventArrived = false;

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
                event.setObjectArray(finalFriendshipEvent);
                event.setSystemArrivalTime(cTime);
                manager.publish();
                startDateTime = new Date();
                startTime = startDateTime.getTime();
                SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
                System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
                firstEvent = false;
            }

            try{
                if (flag == Constants.FRIENDSHIPS) {
                    if(!friendshipLastEventArrived)
                    {
                        friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                        long lastEvent = (Long) friendshipEvent[0];
                        if (lastEvent == -1L)
                        {
                            friendshipEvent = null;
                            friendshipLastEventArrived = true;
                        }
                    }
                }else if (flag == 1){
                    if(!commentsLastEventArrived) {
                        commentEvent = eventBufferList[Constants.COMMENTS].take();
                        long lastEvent = (Long) commentEvent[0];
                        if (lastEvent == -1L) {

                            commentEvent = null;
                            commentsLastEventArrived = true;
                        }
                    }
                }else if (flag == 2) {
                    if (!likesLastEventArrived) {
                        likeEvent = eventBufferList[Constants.LIKES].take();
                        long lastEvent = (Long) likeEvent[0];

                        if (lastEvent == -1L) {
                            likeEvent = null;
                            likesLastEventArrived = true;
                         }
                    }
                }else{

                    if(!friendshipLastEventArrived) {
                        friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                        long lastEvent = (Long) friendshipEvent[0];
                        if (lastEvent == -1L)
                        {
                            friendshipEvent = null;
                            friendshipLastEventArrived = true;
                        }
                    }
                    if(!commentsLastEventArrived) {
                        commentEvent = eventBufferList[Constants.COMMENTS].take();
                        long lastEvent = (Long) commentEvent[0];
                        if (lastEvent == -1L) {

                            commentEvent = null;
                            commentsLastEventArrived = true;
                        }

                    }
                    if(!likesLastEventArrived) {
                        likeEvent = eventBufferList[Constants.LIKES].take();
                        long lastEvent = (Long) likeEvent[0];
                        if (lastEvent == -1L) {
                            likeEvent = null;
                            likesLastEventArrived = true;
                        }
                    }
                }

            }catch (Exception ex){
                ex.printStackTrace();
            }

            long tsFriendship;
            long tsComment;
            long tsLike;

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
                manager.publish();
                flag = Constants.FRIENDSHIPS;
            }else if (tsComment <= tsFriendship && tsComment <= tsLike && tsComment != Long.MAX_VALUE){
                cTime = System.currentTimeMillis();
                DEBSEvent debsEvent = manager.getNextDebsEvent();
                debsEvent.setObjectArray(commentEvent);
                debsEvent.setSystemArrivalTime(cTime);
                manager.publish();
                flag = Constants.COMMENTS;
            }else if (tsLike != Long.MAX_VALUE){
                cTime = System.currentTimeMillis();
                DEBSEvent debsEvent = manager.getNextDebsEvent();
                debsEvent.setObjectArray(likeEvent);
                debsEvent.setSystemArrivalTime(cTime);
                manager.publish();
                flag = Constants.LIKES;
            }

            if (friendshipEvent == null && commentEvent == null && likeEvent == null){
                cTime = System.currentTimeMillis();
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
                manager.publish();
                manager.getDataReadDisruptor().shutdown();
                break;
            }
        }

    }
}