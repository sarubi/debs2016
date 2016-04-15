package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ1;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ2;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by malithjayasinghe on 4/6/16.
 */
public class Query2 {
    private String friendshipFile;
    private String postsFile;
    private String commentsFile;
    private String likesFile;
    private int k;
    private long d;

    public static void main(String[] args){

        File q2 = new File("q2.txt");
        q2.delete();

        if(args.length == 0){
            System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat");
            return;
        }

        Query2 query = new Query2(args);
        query.run();
    }

    public Query2(String[] args){
        friendshipFile = args[0];
        postsFile = args[1];
        commentsFile = args[2];
        likesFile = args[3];
        k = Integer.parseInt(args[4]);
        d = Long.parseLong(args[5]);
    }

    public void run(){


        LinkedBlockingQueue<Object[]> eventBufferListQ2 [] = new LinkedBlockingQueue[3];

        DataLoaderThread dataLoaderThreadFriendships = new DataLoaderThread(friendshipFile, FileType.FRIENDSHIPS,100000,100);
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(commentsFile, FileType.COMMENTS, 100000, 100);
        DataLoaderThread dataLoaderThreadLikes = new DataLoaderThread(likesFile, FileType.LIKES, 100000, 100);

        eventBufferListQ2[0] = dataLoaderThreadFriendships.getEventBuffer();
        eventBufferListQ2[1] = dataLoaderThreadComments.getEventBuffer();
        eventBufferListQ2[2] = dataLoaderThreadLikes.getEventBuffer();

        OrderedEventSenderThreadQ2 orderedEventSenderThreadQ2 = new OrderedEventSenderThreadQ2(eventBufferListQ2, k, d);


        dataLoaderThreadFriendships.start();
        dataLoaderThreadComments.start();
        dataLoaderThreadLikes.start();


        orderedEventSenderThreadQ2.start();


    }

}
