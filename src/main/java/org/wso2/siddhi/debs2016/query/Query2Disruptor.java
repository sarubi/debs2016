package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThread;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by bhagya on 3/30/16.
 */
public class Query2Disruptor {
    private String friendshipFile;
    private String postsFile;
    private String commentsFile;
    private String likesFile;

    public static void main(String[] args){

        File q2 = new File("q2.txt");
        q2.delete();

        if(args.length == 0){
            System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat");
            return;
        }

        Query2Disruptor query2 = new Query2Disruptor(args);
        query2.run();
    }

    public Query2Disruptor(String[] args){
        friendshipFile = args[0];
        postsFile = args[1];
        commentsFile = args[2];
        likesFile = args[3];
    }

    public void run(){


        System.out.println("Incremental data loading is performed.");

        LinkedBlockingQueue<Object[]> eventBufferList [] = new LinkedBlockingQueue[3];


        LinkedBlockingQueue<Object[]> eventBufferListPosts = new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE);
        //Friendships
        DataLoaderThread dataLoaderThreadFriendships = new DataLoaderThread(friendshipFile, eventBufferListPosts, FileType.FRIENDSHIPS);

        //Comments
        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(commentsFile, eventBufferListComments, FileType.COMMENTS);

        //Likes
        LinkedBlockingQueue<Object[]> eventBufferListLikes = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadLikes = new DataLoaderThread(likesFile, eventBufferListLikes, FileType.LIKES);

        eventBufferList[0] = dataLoaderThreadFriendships.getEventBuffer();
        eventBufferList[1] = dataLoaderThreadComments.getEventBuffer();
        eventBufferList[2] = dataLoaderThreadLikes.getEventBuffer();

        OrderedEventSenderThread orderedEventSenderThread = new OrderedEventSenderThread(eventBufferList, null);

        //start the data loading process
        dataLoaderThreadFriendships.start();
        dataLoaderThreadComments.start();
        dataLoaderThreadLikes.start();

        orderedEventSenderThread.start();


        //Just make the main thread sleep infinitely
        //Note that we cannot have an event based mechanism to exit from this infinit loop. It is
        //because even if the data sending thread has completed its task of sending the data to
        //the SiddhiManager, the SiddhiManager object may be conducting the processing of the remaining
        //data. Furthermore, since this is CEP its better have this type of mechanism, rather than
        //terminating once we are done sending the data to the CEP engine.
        while(true){
            try {
                Thread.sleep(Constants.MAIN_THREAD_SLEEP_TIME);
                if (orderedEventSenderThread.doneFlag){
                    System.exit(0);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
