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

        String inStreamDefinition = "@config(async = 'true')define stream postsStream (iij_timestamp float, ts long, post_id long, user_id long, post string, user string);";
        inStreamDefinition += "@config(async = 'true')define stream commentsStream (iij_timestamp float, ts long, comment_id long, user_id long, comment string, user string, comment_replied long, post_commented long);";
        inStreamDefinition += "@config(async = 'true')define stream friendshipsStream (iij_timestamp float, ts long, user_id_1 long, user_id_2 long);";
        inStreamDefinition += "@config(async = 'true')define stream likesStream (iij_timestamp float, ts long, user_id long, comment_id long);";
        inStreamDefinition += "@config(async = 'true')define stream postCommentsStream (iij_timestamp float, ts long, post_id long, post_comment string, comment_id long, comment_replied long);";

        String query = ("@info(name = 'query1') from postsStream  " +
                "select iij_timestamp, ts, post_id, post, -1, -1 " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query2') from commentsStream  " +
                "select iij_timestamp, ts, post_commented, comment, comment_id, comment_replied " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query3') from postCommentsStream#debs2016:ranker(iij_timestamp, ts, post_id, post_comment, comment_id, comment_replied)  " +
                "select iij_timestamp, ts, comment_id, user_id, comment, user, comment_replied, post_commented " +
                "insert into outputStream2;");


        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream1", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        executionPlanRuntime.addCallback("outputStream2", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        System.out.println("Incremental data loading is performed.");
        LinkedBlockingQueue<Object[]> eventBufferListPosts = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);

        //Posts
        DataLoderThread dataLoaderThreadPosts = new DataLoderThread(dataSetFolder + "/posts.dat", eventBufferListPosts, FileType.POSTS);
        InputHandler inputHandlerPosts = executionPlanRuntime.getInputHandler("postsStream");
        EventSenderThread senderThreadPosts = new EventSenderThread(eventBufferListPosts, inputHandlerPosts, Long.MAX_VALUE);

        //Comments
        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoderThread dataLoaderThreadComments = new DataLoderThread(dataSetFolder + "/comments.dat", eventBufferListComments, FileType.COMMENTS);
        InputHandler inputHandlerComments = executionPlanRuntime.getInputHandler("commentsStream");
        EventSenderThread senderThreadComments = new EventSenderThread(eventBufferListComments, inputHandlerComments, Long.MAX_VALUE);

        executionPlanRuntime.start();
        //start the data loading process
        dataLoaderThreadPosts.start();
        dataLoaderThreadComments.start();
        //from here onwards we start sending the events
        senderThreadPosts.start();
        senderThreadComments.start();
    }
}
