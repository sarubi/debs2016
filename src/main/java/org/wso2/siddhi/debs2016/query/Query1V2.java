package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQuery1;
import org.wso2.siddhi.debs2016.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class Query1V2 {
    private static LinkedBlockingQueue<Object[]> eventBufferList = null;
    private String dataSetFolder;
    private String postsFile;
    private String commentsFile;

    public static void main(String[] args){
        if(args.length != 2){
            System.err.println("Usage java org.wso2.siddhi.debs2016.query.Query1V2 Expected Args: <Path to posts.dat, Path to comments.dat>");
            return;
        }

        Query1V2 query1 = new Query1V2(args);
        query1.run();
    }

    public Query1V2(String[] args){
//        dataSetFolder = args[0];
        postsFile = args[0];
        commentsFile = args[1];
    }

    public void run(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream inStream (iij_timestamp long, ts long, user_id long, comment_id long, comment string, user_name string, comment_replied_id long, post_replied_id long, isPostFlag int);";
        inStreamDefinition += "@config(async = 'true')define stream postCommentsStream (iij_timestamp long, ts long, user_id long, comment_id long, comment string, user_name string, comment_replied_id long, post_replied_id long, isPostFlag int );";

        String query = ("@info(name = 'query1') from inStream " +
                "select iij_timestamp, ts, user_id, comment_id, comment, user_name, comment_replied_id, post_replied_id, isPostFlag " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query2') from postCommentsStream#debs2016:rankerQuery1V2(iij_timestamp, ts, user_id, comment_id, comment, user_name, comment_replied_id, post_replied_id, isPostFlag)  " +
                "select result " +
                "insert into query1OutputStream;");

        System.out.println(inStreamDefinition + query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

//        executionPlanRuntime.addCallback("query1OutputStream", new StreamCallback() {
//
//            @Override
//            public void receive(Event[] events) {
//                //EventPrinter.print(events);
//
//            }
//        });

        System.out.println("Incremental data loading is performed.");

        LinkedBlockingQueue<Object[]> eventBufferList [] = new LinkedBlockingQueue[2];
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");


        LinkedBlockingQueue<Object[]> eventBufferListPosts = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);
        //Posts
        DataLoaderThread dataLoaderThreadPosts = new DataLoaderThread(postsFile, eventBufferListPosts, FileType.POSTS);
//        InputHandler inputHandlerPosts = executionPlanRuntime.getInputHandler("postsStream");

        //Comments
        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(commentsFile, eventBufferListComments, FileType.COMMENTS);
//        InputHandler inputHandlerComments = executionPlanRuntime.getInputHandler("commentsStream");


        eventBufferList[0] = dataLoaderThreadPosts.getEventBuffer();
        eventBufferList[1] = dataLoaderThreadComments.getEventBuffer();
//        inputHandler[0] = inputHandlerPosts;
//        inputHandler[1] = inputHandlerComments;

        //EventSenderThread senderThreadComments = new EventSenderThread(dataLoaderThreadComments.getEventBuffer(), inputHandlerComments, Integer.MAX_VALUE);
        OrderedEventSenderThreadQuery1 orderedEventSenderThread = new OrderedEventSenderThreadQuery1(eventBufferList, inputHandler);

        executionPlanRuntime.start();

        //start the data loading process
        dataLoaderThreadPosts.start();
        dataLoaderThreadComments.start();

        //from here onwards we start sending the events
        orderedEventSenderThread.start();

        //Just make the main thread sleep infinitely
        //Note that we cannot have an event based mechanism to exit from this infinit loop. It is
        //because even if the data sending thread has completed its task of sending the data to
        //the SiddhiManager, the SiddhiManager object may be conducting the processing of the remaining
        //data. Furthermore, since this is CEP its better have this type of mechanism, rather than
        //terminating once we are done sending the data to the CEP engine.
        while(true){
            try {
                Thread.currentThread().sleep(Constants.MAIN_THREAD_SLEEP_TIME);
                if (orderedEventSenderThread.doneFlag){
                    System.exit(0);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}