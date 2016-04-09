package org.wso2.siddhi.debs2016.Processors;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.util.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Created by bhagya on 3/30/16.
 */
public class Q2EventManager {
    public Disruptor<DEBSEvent> getDataReadDisruptor() {
        return dataReadDisruptor;
    }



    Disruptor<DEBSEvent> dataReadDisruptor;
    Disruptor<DEBSEvent> outputProcessDisruptor;

    private RingBuffer dataReadBuffer;
    private long startiij_timestamp;
    private long endiij_timestamp;
    private String ts;
    private long duration= 3600000;
    public Graph friendshipGraph ;

    private static long count_1 = 0;
    private static long count_2 = 0;
    private static long count_3 = 0;

    private static long[] count = {0,0};
    private static long[] numberOfOutputs = {0,0};
    private static long[] latencyArray={0,0};

    private CommentStore commentStore ;
    private int k = 1;
   // private static int count = 0;
    long timeDifference = 0; //This is the time difference for this time window.
    long startTime = 0;
    private Date startDateTime;
   // private Long latency = 0L;
   // private Long numberOfOutputs = 0L;
    static int bufferSize = 512;
    private long sequenceNumber;

    /**
     * The constructor
     *
     */
    public Q2EventManager(long[] count,long[] numberOfOutputs,long[] latencyArray){

        this.count=count;
        this.numberOfOutputs=numberOfOutputs;
        this.latencyArray=latencyArray;

        List<Attribute> attributeList = new ArrayList<Attribute>();
        //friendshipGraph = new Graph();
        //commentStore = new CommentStore(duration, friendshipGraph, k);

        //We print the start and the end times of the experiment even if the performance logging is disabled.
        startDateTime = new Date();
        startTime = startDateTime.getTime();
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
        System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
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
        }, bufferSize, Executors.newFixedThreadPool(4), ProducerType.SINGLE, new SleepingWaitStrategy());

        //******************Handler**************************************//

        DEBSEventHandler debsEventHandler1 = new DEBSEventHandler(0);
        DEBSEventHandler debsEventHandler2 = new DEBSEventHandler(1);
        DEBSEventHandler debsEventHandler3 = new DEBSEventHandler(3);
        DEBSEventHandler debsEventHandler4 = new DEBSEventHandler(4);


        dataReadDisruptor.handleEventsWith(debsEventHandler1);
        dataReadDisruptor.handleEventsWith(debsEventHandler2);
        dataReadDisruptor.handleEventsWith(debsEventHandler3);
        dataReadDisruptor.handleEventsWith(debsEventHandler4);

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
     * Print the throughput etc
     *
     */
    private void showFinalStatistics(int handlerID)
    {
       // if(handlerID==0) {
            try {
                StringBuilder builder = new StringBuilder();
                StringBuilder builder1=new StringBuilder();
                BufferedWriter writer;
                File performance = new File("performance.txt");
                writer = new BufferedWriter(new FileWriter(performance, true));
                builder.setLength(0);

                timeDifference = endiij_timestamp - startiij_timestamp;

                Date dNow = new Date();
                SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
                System.out.println("\n\n Query 2 has completed ..........\n\n");
               // System.out.println("Handler ID : "+handlerID);
                builder1.append("Handler ID : "+handlerID);
                builder1.append("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
                builder1.append("Event count : " + count[handlerID]);
                //System.out.println("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
              //  System.out.println("Event count : " + count[handlerID]);

                String timeDifferenceString = String.format("%06d", timeDifference / 1000); //Convert time to seconds
                builder1.append("Total run time : " + timeDifferenceString);
               // System.out.println("Total run time : " + timeDifferenceString);
                builder.append(timeDifferenceString);
                builder.append(" ");
                //System.out.println("Throughput (events/s): " + Math.round((count[handlerID] * 1000.0) / timeDifference));
                builder1.append("Throughput (events/s): " + Math.round((count[handlerID] * 1000.0) / timeDifference));
                //System.out.println("Total Latency " + latencyArray[handlerID]);
                builder1.append("Total Latency " + latencyArray[handlerID]);
                //System.out.println("Total Outputs " + numberOfOutputs[handlerID]);
                builder1.append("Total Outputs " + numberOfOutputs[handlerID]);
                if (numberOfOutputs[handlerID] != 0) {
                    long averageLatency =  (latencyArray[handlerID]) / (numberOfOutputs[handlerID]);
                    String latencyString = String.format("%06d", averageLatency);
                   // System.out.println("Average Latency " + latencyString);
                    builder1.append("Average Latency " + averageLatency);
                    builder.append(latencyString);
                }
                System.out.println(builder1.toString());

                writer.write(builder.toString());
                writer.close();
                System.out.flush();

            } catch (IOException e) {
                e.printStackTrace();
            }
       // }
    }


    /**
     *
     * The debs event handler
     *
     */
    private class DEBSEventHandler implements EventHandler<DEBSEvent>{
        public Graph friendshipGraph ;
        private CommentStore commentStore ;
        private int handlerId;
        private int myHandlerID;
        private long count = 0;
        private long numberOfOutputs = 0;
        private long latencyArray = 0;

        /**
         * The constructor
         *
         * @param myHandlerID the handler id
         */
        public DEBSEventHandler(int myHandlerID){
            this.myHandlerID = myHandlerID;
            friendshipGraph = new Graph();
            commentStore = new CommentStore(duration, friendshipGraph, k);
        }
        @Override
        public void onEvent(DEBSEvent debsEvent, long l, boolean b) throws Exception {
            try{

                Object[] objects = debsEvent.getObjectArray();
                long ts = (Long) objects[1];
                commentStore.cleanCommentStore(ts);

                if(myHandlerID == debsEvent.getHandlerId()||debsEvent.getHandlerId()==-1) {

                    int streamType = (Integer) objects[8];
                    switch (streamType) {
                        case Constants.COMMENTS:
                            long comment_id = (Long) objects[3];
                            String comment = (String) objects[4];
                            commentStore.registerComment(comment_id, ts, comment, false);
                            count++;
                            break;
                        case Constants.FRIENDSHIPS:
                            if (ts == -2) {
                                count--;
                                showFinalStatistics(myHandlerID);
                                commentStore.destroy();
                                if (myHandlerID==0){
                                    count++;
                                }
                                break;
                            } else if (ts == -1) {
                                count--;
                                startiij_timestamp = (Long) debsEvent.getSystemArrivalTime();
                                break;
                            } else {
                                long user_id_1 = (Long) objects[2];
                                long friendship_user_id_2 = (Long) objects[3];
                                friendshipGraph.addEdge(user_id_1, friendship_user_id_2);
                                commentStore.handleNewFriendship(user_id_1, friendship_user_id_2);
                                if (myHandlerID==0){
                                    count++;
                                }
                                break;
                            }
                        case Constants.LIKES:
                            long user_id_1 = (Long) objects[2];
                            long like_comment_id = (Long) objects[3];
                            commentStore.registerLike(like_comment_id, user_id_1);
                            count++;
                            break;
                    }

                    if (ts != -2 && ts != -1) {
                        Long endTime = commentStore.computeKLargestComments(" : ", false, true);

                        if (endTime != -1L) {
                            latencyArray += (endTime - (Long) debsEvent.getSystemArrivalTime());
                            numberOfOutputs++;
                        }

                        endiij_timestamp = System.currentTimeMillis();
                    }
                }

            }catch (Exception e)
            {
                e.printStackTrace();
            }

        }
    }

}

