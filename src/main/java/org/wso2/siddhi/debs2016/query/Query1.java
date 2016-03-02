package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.debs2016.input.DataLoderThread;
import org.wso2.siddhi.debs2016.input.EventSenderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class Query1 {
    private static LinkedBlockingQueue<Object[]> eventBufferList = null;

    public static void main(String[] args){
        Query1 query1 = new Query1(args);
        query1.run();
    }

    public Query1(String[] args){

    }

    public void run(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream postsStream (ts long, post_id long, user_id long, post string, user string, iij_timestamp float);";
        String query = ("@info(name = 'query1') from postsStream  " +
                "select ts, post_id, user_id, post, user, iij_timestamp " +
                "insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        LinkedBlockingQueue<Object[]> eventBufferList = new LinkedBlockingQueue<Object[]>();

        System.out.println("Incremental data loading is performed.");
        eventBufferList = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);
        DataLoderThread dataLoaderThread = new DataLoderThread("/home/miyurud/DEBS2016/DataSet/data/posts.dat", eventBufferList, FileType.POSTS);
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("postsStream");
        EventSenderThread senderThread = new EventSenderThread(eventBufferList, inputHandler, 100);
        executionPlanRuntime.start();
        //start the data loading process
        dataLoaderThread.start();
        //from here onwards we start sending the events
        senderThread.start();
    }
}
