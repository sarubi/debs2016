package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.EventSenderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class Query1 {
    private static LinkedBlockingQueue<Object[]> eventBufferList = null;
    private String dataSetFolder;

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Usage java org.wso2.siddhi.debs2016.query.Query1 <full path to data set folder>");
            return;
        }

        Query1 query1 = new Query1(args);
        query1.run();
    }

    public Query1(String[] args){
        dataSetFolder = args[0];
    }

    public void run(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream postsStream (iij_timestamp long, ts long, post_id long, user_id long, post string, user string);";
        inStreamDefinition += "@config(async = 'true')define stream commentsStream (iij_timestamp long, ts long, user_id long, comment_id long, comment string, user string, comment_replied long, post_commented long);";
        inStreamDefinition += "@config(async = 'true')define stream postCommentsStream (iij_timestamp long, ts long, post_id long, comment_id long, comment_replied_id long, user_id long, user string, isPostFlag bool );";

        String query = ("@info(name = 'query1') from postsStream " +
                "select iij_timestamp, ts, post_id, -1l as comment_id, -1l as comment_replied_id, user_id, user, true as isPostFlag " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query2') from commentsStream  " +
                "select iij_timestamp, ts, post_commented as post_id, comment_id, comment_replied as comment_replied_id, user_id, user, false as isPostFlag " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query3') from postCommentsStream#debs2016:rankerQuery1(iij_timestamp, ts, post_id, comment_id, comment_replied_id, user_id, user, isPostFlag)  " +
                "select iij_timestamp, ts, post_id, comment_id, comment_replied_id " +
                "insert into query1OutputStream;");

        System.out.println(inStreamDefinition+query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition+query);

        executionPlanRuntime.addCallback("query1OutputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        System.out.println("Incremental data loading is performed.");

        LinkedBlockingQueue<Object[]> eventBufferListPosts = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);
        //Posts
        DataLoaderThread dataLoaderThreadPosts = new DataLoaderThread(dataSetFolder + "/posts.dat", eventBufferListPosts, FileType.POSTS);
        InputHandler inputHandlerPosts = executionPlanRuntime.getInputHandler("postsStream");
        EventSenderThread senderThreadPosts = new EventSenderThread(dataLoaderThreadPosts.getEventBuffer(), inputHandlerPosts, Integer.MAX_VALUE);

        //Comments
        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(dataSetFolder + "/comments.dat", eventBufferListComments, FileType.COMMENTS);
        InputHandler inputHandlerComments = executionPlanRuntime.getInputHandler("commentsStream");
        EventSenderThread senderThreadComments = new EventSenderThread(dataLoaderThreadComments.getEventBuffer(), inputHandlerComments, Integer.MAX_VALUE);

        executionPlanRuntime.start();

        //start the data loading process
        dataLoaderThreadPosts.start();
        dataLoaderThreadComments.start();

        //from here onwards we start sending the events
        senderThreadPosts.start();
        senderThreadComments.start();

        //Just make the main thread sleep infinitely
        //Note that we cannot have an event based mechanism to exit from this infinit loop. It is
        //because even if the data sending thread has completed its task of sending the data to
        //the SiddhiManager, the SiddhiManager object may be conducting the processing of the remaining
        //data. Furthermore, since this is CEP its better have this type of mechanism, rather than
        //terminating once we are done sending the data to the CEP engine.
        while(true){
            try {
                Thread.sleep(Constants.MAIN_THREAD_SLEEP_TIME);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}