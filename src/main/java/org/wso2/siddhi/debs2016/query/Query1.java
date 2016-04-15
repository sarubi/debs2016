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
public class Query1 {
    private String friendshipFile;
    private String postsFile;
    private String commentsFile;
    private String likesFile;
    private String[] args;
    private OrderedEventSenderThreadQ1 orderedEventSenderThreadQ1;
    DataLoaderThread dataLoaderThreadComments ;
    DataLoaderThread dataLoaderThreadPosts;

    /**
     * The main method
     *
     * @param args arguments
     */
    public static void main(String[] args){

        File q1 = new File("q1.txt");
        q1.delete();

        File performance = new File("performance.txt");
        performance.delete();

        if(args.length == 0){
            System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat");
            return;
        }
        Query1 query = new Query1(args);
        query.run();
    }

    /**
     * The constructor
     *
     * @param args arguments
     */
    public Query1(String[] args){
        friendshipFile = args[0];
        postsFile = args[1];
        commentsFile = args[2];
        likesFile = args[3];
        this.args = args;

        LinkedBlockingQueue<Object[]> eventBufferListQ1 [] = new LinkedBlockingQueue[2];
        orderedEventSenderThreadQ1 = new OrderedEventSenderThreadQ1(eventBufferListQ1);
        dataLoaderThreadComments = new DataLoaderThread(commentsFile, FileType.COMMENTS,1000,10);
        dataLoaderThreadPosts = new DataLoaderThread(postsFile, FileType.POSTS,1000,10);
        eventBufferListQ1 [0] = dataLoaderThreadPosts.getEventBuffer();
        eventBufferListQ1 [1] = dataLoaderThreadComments.getEventBuffer();
    }

    public void run(){

        dataLoaderThreadComments.start();
        dataLoaderThreadPosts.start();
        orderedEventSenderThreadQ1.start();
    }


}
