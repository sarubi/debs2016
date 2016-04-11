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

            Thread.sleep(1000);
            while(true) {
                sort1();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Sort the final output
     */
    private void sort()
    {
        try {

            while (eventBufferList1.size() > 0 && eventBufferList2.size() > 0 && eventBufferList3.size() > 0 && eventBufferList4.size() > 0) {

                componentSizeCommentMap = TreeMultimap.create(Comparator.<Long>reverseOrder(), Comparator.<String>naturalOrder());

                System.out.println("size buffer 1 = " + eventBufferList1.size());
                System.out.println("size buffer 2 = " + eventBufferList2.size());
                System.out.println("size buffer 3 = " + eventBufferList3.size());
                System.out.println("size buffer 4 = " + eventBufferList4.size());
                System.out.println("\n");

                KLargestEvent event1Object = eventBufferList1.poll(500, TimeUnit.MILLISECONDS);
                KLargestEvent event2Object = eventBufferList2.poll(500, TimeUnit.MILLISECONDS);
                KLargestEvent event3Object = eventBufferList3.poll(500, TimeUnit.MILLISECONDS);
                KLargestEvent event4Object = eventBufferList4.poll(500, TimeUnit.MILLISECONDS);


                if (event1Object != null) {
                    Multimap<Long, String> event = event1Object.getkLargestComment();
                    componentSizeCommentMap.putAll(event);
                }

                if (event2Object != null) {
                    Multimap<Long, String> event = event2Object.getkLargestComment();
                    componentSizeCommentMap.putAll(event);
                }

                if (event3Object != null) {
                    Multimap<Long, String> event = event3Object.getkLargestComment();
                    componentSizeCommentMap.putAll(event);
                }

                if (event4Object != null) {
                    Multimap<Long, String> event = event4Object.getkLargestComment();
                    componentSizeCommentMap.putAll(event);
                }
            }


            //tsTriggeredChange = event1Object.getTimeStamp(); //All events will have same timestamp

            //TODO: Verify that .putAll() method adds all elements in order of the comparator

            //printKLargestComments(",", true, true);  //tsTriggeredChange = event1Object.getTimeStamp(); //All events will have same timestamp

            //TODO: Verify that .putAll() method adds all elements in order of the comparator

            //printKLargestComments(",", true, true);
        }catch (Exception e)
        {
            e.printStackTrace();;
        }

    }


    /**
     * Sort the final output
     */
    private void sort1()
    {
        try {


            componentSizeCommentMap = TreeMultimap.create(Comparator.<Long>reverseOrder(), Comparator.<String>naturalOrder());
            Long timeStampArray [] = new Long[4];

//                System.out.println("size buffer 1 = " + eventBufferList1.size());
//                System.out.println("size buffer 2 = " + eventBufferList2.size());
//                System.out.println("size buffer 3 = " + eventBufferList3.size());
//                System.out.println("size buffer 4 = " + eventBufferList4.size());
//
//                System.out.println("\n");

            KLargestEvent event1Object = eventBufferList1.take();
            KLargestEvent event2Object = eventBufferList2.take();
            KLargestEvent event3Object = eventBufferList3.take();
            KLargestEvent event4Object = eventBufferList4.take();

            timeStampArray[0] =  event1Object.getTimeStamp();
            timeStampArray[1] =  event2Object.getTimeStamp();
            timeStampArray[2] =  event3Object.getTimeStamp();
            timeStampArray[3] =  event4Object.getTimeStamp();

            long minTimeStamp = timeStampArray[0];
            int minTimeStampIndex = 0;

              /* for(int i=1; i<timeStampArray.length+1; i++)
               {
                   if(minTimeStamp > timeStampArray[i] )
               }*/






            Multimap<Long, String> event1 = event1Object.getkLargestComment();
            componentSizeCommentMap.putAll(event1);



            Multimap<Long, String> event2 = event2Object.getkLargestComment();
            componentSizeCommentMap.putAll(event2);



            Multimap<Long, String> event3 = event3Object.getkLargestComment();
            componentSizeCommentMap.putAll(event3);



            Multimap<Long, String> event4 = event4Object.getkLargestComment();
            componentSizeCommentMap.putAll(event4);




            //tsTriggeredChange = event1Object.getTimeStamp(); //All events will have same timestamp

            //TODO: Verify that .putAll() method adds all elements in order of the comparator

            printKLargestComments(",", true, false);  //tsTriggeredChange = event1Object.getTimeStamp(); //All events will have same timestamp

            //TODO: Verify that .putAll() method adds all elements in order of the comparator

            //printKLargestComments(",", true, true);
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
