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
public class OutputProcessor implements Runnable {

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

    public void run()
    {
        try {
        // do the sorting here...
        // write the final output to a file
        // compute stats etc
            componentSizeCommentMap = TreeMultimap.create(Comparator.<Long>reverseOrder(), Comparator.<String>naturalOrder());
            KLargestEvent event1Object = eventBufferList1.poll(100, TimeUnit.MILLISECONDS);
            Multimap<Long, String> event1 = event1Object.getkLargestComment();
            Multimap<Long, String> event2 = eventBufferList1.poll(100, TimeUnit.MILLISECONDS).getkLargestComment();
            Multimap<Long, String> event3 = eventBufferList1.poll(100, TimeUnit.MILLISECONDS).getkLargestComment();
            Multimap<Long, String> event4 = eventBufferList1.poll(100, TimeUnit.MILLISECONDS).getkLargestComment();
            tsTriggeredChange = event1Object.getTimeStamp(); //All events will have same timestamp

            componentSizeCommentMap.putAll(event1);
            componentSizeCommentMap.putAll(event2);
            componentSizeCommentMap.putAll(event3);
            componentSizeCommentMap.putAll(event4);
            //TODO: Verify that .putAll() method adds all elements in order of the comparator

            printKLargestComments(",", true, true);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }


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


    public void add(KLargestEvent event, int handlerID) {

        try {

            if (handlerID == 1) {
                eventBufferList1.put(event);
            } else if (handlerID == 2) {
                eventBufferList2.put(event);
            } else if (handlerID == 3) {
                eventBufferList3.put(event);
            } else if (handlerID == 4) {
                eventBufferList4.put(event);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
