package org.wso2.siddhi.debs2016.Processors;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;

public class Q2EventManager {

    private static final StringBuilder builder = new StringBuilder();
    private Disruptor<DEBSEvent> dataReadDisruptor;
    private RingBuffer dataReadBuffer;
    private static long startTimestamp;
    private static long endTimestamp;
    private final Graph friendshipGraph ;
    private final CommentStore commentStore ;
    private static int count;
    private static long latency;
    private static long  numberOfOutputs;
    private long sequenceNumber;
    public static volatile boolean Q2_COMPLETED = false;


    /**
     * The constructor
     *
     */
    public Q2EventManager(int k, long duration){
        friendshipGraph = new Graph();
        commentStore = new CommentStore(duration, friendshipGraph, k);
    }

    /**
     * Gets the data reader distruptor
     *
     * @return the data reader distruptor
     */
    public Disruptor<DEBSEvent> getDataReadDisruptor() {
        return dataReadDisruptor;
    }

    /**
     *
     * Starts the distruptor
     *
     */
    public void run() {
        int bufferSize = 512;
        dataReadDisruptor = new Disruptor<>(DEBSEvent::new, bufferSize, Executors.newFixedThreadPool(1), ProducerType.SINGLE, new SleepingWaitStrategy());
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
     * Writes the output to the file
     */
    public static void writeOutput() {
        File performance = new File("performance.txt");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(performance, true))) {
            String result = builder.toString();
            writer.write(result);
            writer.close();
            System.out.flush();
        } catch (Exception e)
        {
            e.printStackTrace();
        }

    }
    /**
     *
     * Print the throughput etc
     *
     */
    private synchronized void showFinalStatistics()
    {
        try {
            commentStore.destroy();
            builder.setLength(0);
            long timeDifference = endTimestamp - startTimestamp;
            Date dNow = new Date();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
            System.out.println("Query 2 completed .....at : " + dNow.getTime() + "--" + simpleDateFormat.format(dNow));
            System.out.println("Event count : " + count);
            String timeDifferenceString = Float.toString(((float) timeDifference /1000)) + "000000";
            System.out.println("Total run time : " + timeDifferenceString.substring(0, 7));
            builder.append(timeDifferenceString.substring(0, 7));
            builder.append(", ");

            System.out.println("Throughput (events/s): " + Math.round((count * 1000.0) / timeDifference));
            System.out.println("Total Latency " + latency);
            System.out.println("Total Outputs " + numberOfOutputs);
            if (numberOfOutputs != 0) {
                float temp = ((float)latency/numberOfOutputs)/1000;
                BigDecimal averageLatency = new BigDecimal(temp);
                String latencyString = averageLatency.toPlainString() + "000000";
                System.out.println("Average Latency " + latencyString.substring(0, 7));
                builder.append(latencyString.substring(0, 7));
            } else {
                String latencyString = "000000";
                builder.append(latencyString);
            }
        }finally {
            Q2EventManager.Q2_COMPLETED = true;
            if(Q1EventManager.Q1_COMPLETED)
            {
                Q1EventManager.writeOutput();
                writeOutput();
                System.exit(0);
            }
        }
    }


    /**
     *
     * The debs event handler
     *
     */
    private class DEBSEventHandler implements EventHandler<DEBSEvent>{
        @Override
        public void onEvent(DEBSEvent debsEvent, long l, boolean b) throws Exception {
            try{

                Object [] objects = debsEvent.getObjectArray();

                long logicalTimestamp = (Long) objects[1];
                //Note that we cannot cast int to enum type. Java enums are classes. Hence we cannot cast them to int.
                int streamType = (Integer) objects[8];
                commentStore.cleanCommentStore(logicalTimestamp);
                count++;

                switch (streamType) {
                    case Constants.COMMENTS:
                        long commentId = (Long) objects[3];
                        String comment = (String) objects[4];
                        commentStore.registerComment(commentId, logicalTimestamp, comment);
                        break;
                    case Constants.FRIENDSHIPS:
                        if (logicalTimestamp == -2){
                            count--;
                            showFinalStatistics();
                            commentStore.destroy();
                            break;
                        }else if (logicalTimestamp == -1) {
                            count--;
                            startTimestamp = debsEvent.getSystemArrivalTime();
                            break;
                        }else{
                            long userOneId = (Long) objects[2];
                            long userTwoId = (Long) objects[3];
                            friendshipGraph.addEdge(userOneId, userTwoId);
                            commentStore.handleNewFriendship(userOneId, userTwoId);
                            break;
                        }
                    case Constants.LIKES:
                        long userId = (Long) objects[2];
                        long likeCommentId = (Long) objects[3];
                        commentStore.registerLike(likeCommentId, userId);
                        break;
                }

                if (logicalTimestamp != -2 && logicalTimestamp != -1){
                    Long endTime = commentStore.computeKLargestComments("," ,false, true);

                    if (endTime != -1L){
                        latency += (endTime - debsEvent.getSystemArrivalTime());
                        numberOfOutputs++;
                    }

                    endTimestamp = System.currentTimeMillis();
                }

            }catch (Exception e)
            {
                e.printStackTrace();
            }

        }
    }

}

