package org.wso2.siddhi.debs2016.query;


import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.EventSenderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;

public class Query2 {
    private String dataSetFolder;

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Usage java org.wso2.siddhi.debs2016.query.Query2 <full path to data set folder>");
            return;
        }

        Query2 query2 = new Query2(args);
        query2.run();
    }

    public Query2(String[] args){
        dataSetFolder = args[0];
    }

    public void run(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream friendshipsStream (iij_timestamp long, ts long, user_id_1 long, user_id_2 long);";
        inStreamDefinition += "@config(async = 'true')define stream commentsStream (iij_timestamp long, ts long, comment_id long, user_id long, comment string, user string, comment_replied long, post_commented long);";
        inStreamDefinition += "@config(async = 'true')define stream likesStream (iij_timestamp long, ts long, user_id long, comment_id long);";

        String query = ("@info(name = 'query1') from friendshipsStream " +
                "select * " +
                "insert into likesFriendshipsCommentsStream;");

        query += ("@info(name = 'query2') from commentsStream  " +
                "select * " +
                "insert into likesFriendshipsCommentsStream;");

        query += ("@info(name = 'query3') from likesStream  " +
                "select * " +
                "insert into likesFriendshipsCommentsStream;");

        query += ("@info(name = 'query4') from likesFriendshipsCommentsStream#debs2016:rankerQuery2(iij_timestamp, ts)  " +
                "select * " +
                "insert into query2OutputStream;");


        System.out.println(inStreamDefinition+query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition+query);

        executionPlanRuntime.addCallback("query2OutputStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
            }
        });

        System.out.println("Incremental data loading is performed.");

        LinkedBlockingQueue<Object[]> eventBufferListPosts = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);
        //Friendships
        DataLoaderThread dataLoaderThreadFriendships = new DataLoaderThread(dataSetFolder + "/friendships.dat", eventBufferListPosts, FileType.FRIENDSHIPS);
        InputHandler inputHandlerFriendships = executionPlanRuntime.getInputHandler("friendshipsStream");
        EventSenderThread senderThreadFriendships = new EventSenderThread(dataLoaderThreadFriendships.getEventBuffer(), inputHandlerFriendships, Integer.MAX_VALUE);

        //Comments
        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(dataSetFolder + "/comments.dat", eventBufferListComments, FileType.COMMENTS);
        InputHandler inputHandlerComments = executionPlanRuntime.getInputHandler("commentsStream");
        EventSenderThread senderThreadComments = new EventSenderThread(dataLoaderThreadComments.getEventBuffer(), inputHandlerComments, Integer.MAX_VALUE);

        //Likes
        LinkedBlockingQueue<Object[]> eventBufferListLikes = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadLikes = new DataLoaderThread(dataSetFolder + "/likes.dat", eventBufferListComments, FileType.LIKES);
        InputHandler inputHandlerLikes = executionPlanRuntime.getInputHandler("likesStream");
        EventSenderThread senderThreadLikes = new EventSenderThread(dataLoaderThreadLikes.getEventBuffer(), inputHandlerLikes, Integer.MAX_VALUE);

        executionPlanRuntime.start();

        //start the data loading process
        dataLoaderThreadFriendships.start();
        dataLoaderThreadComments.start();
        dataLoaderThreadLikes.start();
        //from here onwards we start sending the events
        senderThreadFriendships.start();
        senderThreadComments.start();
        senderThreadLikes.start();


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
