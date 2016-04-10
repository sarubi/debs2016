package org.wso2.siddhi.debs2016.Processors;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by malithjayasinghe on 4/9/16.
 */
public class OutputProcessor implements Runnable {


    private LinkedBlockingQueue<KLargestEvent> eventBufferList1 = new LinkedBlockingQueue<KLargestEvent>();
    private LinkedBlockingQueue<KLargestEvent> eventBufferList2 = new LinkedBlockingQueue<KLargestEvent>();
    private LinkedBlockingQueue<KLargestEvent> eventBufferList3 = new LinkedBlockingQueue<KLargestEvent>();
    private LinkedBlockingQueue<KLargestEvent> eventBufferList4 = new LinkedBlockingQueue<KLargestEvent>();

    public void run()
    {

        // do the sorting here...
        // write the final output to a file
        // compute stats etc

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
