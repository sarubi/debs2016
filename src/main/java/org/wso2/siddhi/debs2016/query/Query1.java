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
        inStreamDefinition += "@config(async = 'true')define stream commentsStream (iij_timestamp long, ts long, comment_id long, user_id long, comment string, user string, comment_replied long, post_commented long);";
        inStreamDefinition += "@config(async = 'true')define stream friendshipsStream (iij_timestamp long, ts long, user_id_1 long, user_id_2 long);";
        inStreamDefinition += "@config(async = 'true')define stream likesStream (iij_timestamp long, ts long, user_id long, comment_id long);";
        inStreamDefinition += "@config(async = 'true')define stream postCommentsStream (iij_timestamp long, ts long, post_id long, comment_id long, comment_replied_id long, isPostFlag bool );";

        String query = ("@info(name = 'query1') from postsStream " +
                "select iij_timestamp, ts, post_id, -1l as comment_id, -1l as comment_replied_id, true as isPostFlag " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query2') from commentsStream  " +
                "select iij_timestamp, ts, post_commented as post_id, comment_id, comment_replied as comment_replied_id, false as isPostFlag " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query3') from postCommentsStream#debs2016:ranker(iij_timestamp, ts, post_id, comment_id, comment_replied_id, isPostFlag)  " +
                "select iij_timestamp, ts, post_id, comment_id, comment_replied_id " +
                "insert into query1OutputStream;");

        System.out.println(inStreamDefinition+query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition+query);

//        executionPlanRuntime.addCallback("outputStream1", new StreamCallback() {
//
//            @Override
//            public void receive(Event[] events) {
//                EventPrinter.print(events);
//            }
//        });

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
        EventSenderThread senderThreadPosts = new EventSenderThread(dataLoaderThreadPosts.getEventBuffer(), inputHandlerPosts, 100);

        //Comments
        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(dataSetFolder + "/comments.dat", eventBufferListComments, FileType.COMMENTS);
        InputHandler inputHandlerComments = executionPlanRuntime.getInputHandler("commentsStream");
        EventSenderThread senderThreadComments = new EventSenderThread(dataLoaderThreadComments.getEventBuffer(), inputHandlerComments, 100);

        executionPlanRuntime.start();

        //start the data loading process
        dataLoaderThreadPosts.start();
        dataLoaderThreadComments.start();

        //from here onwards we start sending the events
        senderThreadPosts.start();
        senderThreadComments.start();
    }
}
