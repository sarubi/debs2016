package org.wso2.siddhi.debs2016.Processors;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by malithjayasinghe on 4/9/16.
 */
public class OutputProcessor extends Thread {

    private String[] previousKcomments;
    Multimap<Long, String> componentSizeCommentMap;
    private String[] kComments;
    private int k = 2;
    private long tsTriggeredChange;
    private StringBuilder builder = new StringBuilder();
    private BufferedWriter writer;
    private LinkedBlockingQueue<KLargestEvent> eventBufferList1 = new LinkedBlockingQueue<KLargestEvent>();
    private LinkedBlockingQueue<KLargestEvent> eventBufferList2 = new LinkedBlockingQueue<KLargestEvent>();
    private LinkedBlockingQueue<KLargestEvent> eventBufferList3 = new LinkedBlockingQueue<KLargestEvent>();
    private LinkedBlockingQueue<KLargestEvent> eventBufferList4 = new LinkedBlockingQueue<KLargestEvent>();
    private long startiij_timestamp;
    private long endiij_timestamp;

    public void run()
    {
        try {
            while(true) {
                process();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Sort the final output
     */
    private void process()
    {
        try {
            componentSizeCommentMap = TreeMultimap.create(Comparator.<Long>reverseOrder(), Comparator.<String>naturalOrder());
            Long timeStampArray [] = new Long[4];

            KLargestEvent event1Object = eventBufferList1.take();
            KLargestEvent event2Object = eventBufferList2.take();
            KLargestEvent event3Object = eventBufferList3.take();
            KLargestEvent event4Object = eventBufferList4.take();

            timeStampArray[0] =  event1Object.getTimeStamp();
            timeStampArray[1] =  event2Object.getTimeStamp();
            timeStampArray[2] =  event3Object.getTimeStamp();
            timeStampArray[3] =  event4Object.getTimeStamp();


            Multimap<Long, String> event1 = event1Object.getkLargestComment();
            componentSizeCommentMap.putAll(event1);

            Multimap<Long, String> event2 = event2Object.getkLargestComment();
            componentSizeCommentMap.putAll(event2);

            Multimap<Long, String> event3 = event3Object.getkLargestComment();
            componentSizeCommentMap.putAll(event3);

            Multimap<Long, String> event4 = event4Object.getkLargestComment();
            componentSizeCommentMap.putAll(event4);

            printKLargestComments(",", false, false);  //tsTriggeredChange = event1Object.getTimeStamp(); //All events will have same timestamp

            //TODO: Verify that .putAll() method adds all elements in order of the comparator
        }catch (Exception e)
        {
            e.printStackTrace();;
        }

    }

    /**
     * Print/write the final output to a file
     *
     * @param delimiter the delimiter to use
     * @param printKComments whether to print the output to screen
     * @param writeToFile whether the write the output to a file
     * @return the system time
     */
    public long printKLargestComments(String delimiter, boolean printKComments, boolean writeToFile) {

        try {
            if (hasKLargestCommentsChanged()) {
                builder.setLength(0);
                builder.append(tsTriggeredChange);
                for (String print : previousKcomments) {
                    builder.append(delimiter + print);
                }
                builder.append("\n");
                if (printKComments) {
                    System.out.println(builder.toString());
                }
                if (writeToFile) {
                    writer.write(builder.toString());
                }
                return System.currentTimeMillis();
            }
        }catch(IOException e)
        {
            e.printStackTrace();
        }
        return  -1L;
    }


    /**
     * Check if k largest comments have changed
     *
     *
     * @return true if there is change false otherwise
     */
    private boolean hasKLargestCommentsChanged()
    {
         /*Check if a change has taken place in K largest comments*/
        boolean debug = true;

        boolean flagChange = false;
        kComments = new String[k];
        if (componentSizeCommentMap != null) {
            int limit = (k <= componentSizeCommentMap.size() ? k : componentSizeCommentMap.size());
            int i = 0;

            for (String comment: componentSizeCommentMap.values()){
                kComments[i] = comment;
                if (previousKcomments == null) {
                    flagChange = true;
                } else if (!(kComments[i].equals(previousKcomments[i]))) {
                    flagChange = true;
                }
                i++;
                if (i == limit){
                    break;
                }
            }
            if (limit == componentSizeCommentMap.size()) {
                for (int j = componentSizeCommentMap.size(); j < k; j++) {
                    kComments[j] = "-";
                }
            }

            if (flagChange) {
                previousKcomments = kComments;
            }
        }
        return flagChange;
    }


    /**
     * Adds a new k largest event to the appropiate buffer
     *
     * @param event the event to be added
     * @param handlerID the handler id
     */
    public void add(KLargestEvent event, int handlerID) {

        try {

            if (handlerID == 0) {
                eventBufferList1.put(event);
            } else if (handlerID == 1) {
                eventBufferList2.put(event);
            } else if (handlerID == 2) {
                eventBufferList3.put(event);
            } else if (handlerID == 3) {
                eventBufferList4.put(event);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
