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

        String inStreamDefinition = "@config(async = 'true')define stream postsStream (iij_timestamp float, ts long, post_id long, user_id long, post string, user string);";
        inStreamDefinition += "@config(async = 'true')define stream commentsStream (iij_timestamp float, ts long, comment_id long, user_id long, comment string, user string, comment_replied long, post_commented long);";

        String query = ("@info(name = 'query1') from postsStream  " +
                "select iij_timestamp, ts, post_id, user_id, post, user " +
                "insert into outputStream1;");

        query += ("@info(name = 'query2') from commentsStream  " +
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

        DataLoderThread dataLoaderThreadPosts = new DataLoderThread("/home/miyurud/DEBS2016/DataSet/data/posts.dat", eventBufferListPosts, FileType.POSTS);
        InputHandler inputHandlerPosts = executionPlanRuntime.getInputHandler("postsStream");
        EventSenderThread senderThreadPosts = new EventSenderThread(eventBufferListPosts, inputHandlerPosts, 100);

        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoderThread dataLoaderThreadComments = new DataLoderThread("/home/miyurud/DEBS2016/DataSet/data/comments.dat", eventBufferListComments, FileType.COMMENTS);
        InputHandler inputHandlerComments = executionPlanRuntime.getInputHandler("commentsStream");
        EventSenderThread senderThreadComments = new EventSenderThread(eventBufferListComments, inputHandlerComments, 100);

        executionPlanRuntime.start();
        //start the data loading process
        dataLoaderThreadPosts.start();
        dataLoaderThreadComments.start();
        //from here onwards we start sending the events
        senderThreadPosts.start();
        senderThreadComments.start();
    }
}
