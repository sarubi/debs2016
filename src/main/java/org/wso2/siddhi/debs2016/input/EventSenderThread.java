package org.wso2.siddhi.debs2016.input;

import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2016.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

public class EventSenderThread extends Thread {
    private LinkedBlockingQueue<Object[]> eventBufferList;
    private InputHandler inputHandler;
    private Date startDateTime;
    private long EVENT_COUNT;

    /**
     * The constructor
     *
     * @param eventBuffer the eventBuffer which containts the events to be sent to the processing engine
     * @param inputHandler the input handler of the execution plan
     * @param eventCount the event count
     */
    public EventSenderThread(LinkedBlockingQueue<Object[]> eventBuffer, InputHandler inputHandler, long eventCount){
        super("Event Sender");
        this.eventBufferList = eventBuffer;
        this.inputHandler = inputHandler;
        this.EVENT_COUNT = eventCount;
    }

    public void run(){
        Object[] event = null;
        long count = 1;
        long timeDifferenceFromStart = 0;
        long timeDifference = 0; //This is the time difference for this time window.
        long currentTime = 0;
        long prevTime = 0;
        long startTime = 0;
        long cTime = 0;

        //Special note : Originally we need not subtract 1. However, due to some reason if there are n events in the input data set that are
        //pumped to the eventBufferList queue, only (n-1) is read. Therefore, we have -1 here.
        //final int EVENT_COUNT = Integer.parseInt(Config.getConfigurationInfo("org.wso2.siddhi.debs2015.dataset.size")) - 1;

        boolean firstEvent = true;
        float percentageCompleted = 0;

        while(true){
            try {
                event = (Object[]) eventBufferList.take();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

            try {
                //In the case of input performance measurements, we mark the time when the first tuple gets emitted to the SiddhiManager.

                if(firstEvent){
                    //We print the start and the end times of the experiment even if the performance logging is disabled.
                    startDateTime = new Date();
                    startTime = startDateTime.getTime();
                    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
                    System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));
                    firstEvent = false;
                }

                cTime = System.currentTimeMillis();
                event[Constants.INPUT_INJECTION_TIMESTAMP_FIELD]	= cTime; //This corresponds to the iij_timestamp

                inputHandler.send(cTime, event);
                count++;

                if (count > EVENT_COUNT ){
                    percentageCompleted = ((float)count/ EVENT_COUNT);
                    currentTime = System.currentTimeMillis();
                    timeDifferenceFromStart = (currentTime - startTime);
                    timeDifference = currentTime - prevTime;

                    //<time from start(ms)><time from start(s)><aggregate throughput (events/s)><throughput in this time window (events/s)><percentage completed (%)>
                    //aggregateInputList.add(timeDifferenceFromStart + "," + Math.round(timeDifferenceFromStart/1000) + "," + Math.round(count * 1000.0 / timeDifferenceFromStart) + "," + Math.round(Constants.STATUS_REPORTING_WINDOW_INPUT  * 1000.0/timeDifference) + "," + percentageCompleted);
                    //At this moment we are done with sending all the events from the queue. Now we are about to complete the experiment.

                    Date dNow = new Date();
                    SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
                    System.out.println("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
                    System.out.println("Event count : " + count);
                    timeDifferenceFromStart = dNow.getTime() - startDateTime.getTime();
                    System.out.println("Total run time : " + timeDifferenceFromStart);
                    System.out.println("Average input data rate (events/s): " + Math.round((count * 1000.0)/timeDifferenceFromStart));
                    System.out.flush();
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}