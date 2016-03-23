package org.wso2.siddhi.debs2016.query;


import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThread;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;
/**
 Query 2

 This query addresses the change of interests with large communities. It represents a version of query type 2 from the 2014 SIGMOD Programming contest.
 Unlike in the SIGMOD problem, the version for the DEBS Grand Challenge focuses on the dynamic change of query results over time, i.e., calls for a
 continuous evaluation of the results.

 Goal of the query:
 Given an integer k and a duration d (in seconds), find the k comments with the largest range, where the range of a comment is defined as the size
 of the largest connected component in the graph defined by persons who (a) have liked that comment (see likes, comments), (b) where the comment was created not more than d seconds ago, and (c) know each other (see friendships).

 Parameters: k, d

 Input Streams: likes, friendships, comments

 Output:

 The output includes a single timestamp ts and exactly k strings per line. The timestamp and the strings should be separated by commas.
 The k strings represent comments, ordered by range from largest to smallest, with ties broken by lexicographical ordering (ascending).
 The k strings and the corresponding timestamp must be printed only when some input triggers a change of the output, as defined above.
 If less than k strings can be determined, the character “-” (a minus sign without the quotations) should be printed in place of each missing string.

 The field ts corresponds to the timestamp of the input data item that triggered an output update. For instance, a new friendship relation may
 change the size of a community with a shared interest and hence may change the k strings. The timestamp of the event denoting the added friendship
 relation is then the timestamp ts for that line's output. Also, the output must be updated when the results change due to the progress of time, e.g.,
 when a comment is older that d. Specifically, if the update is triggered by an event leaving a time window at t2 (i.e., t2 = timestamp of the event +
 window size), the timestamp for the update is t2. As in Query 1, it is needless to say that timestamps refer to the logical time of the input data
 streams, rather than on the system clock.

 In summary, the output is specified as follows:

 ts: the timestamp of thetuple event that triggers a change in the output.
 comments_1,...,comment_k: top k comments ordered by range, starting with the largest range (comment_1).
 Sample output tuples for the query with k=3 could look as follows:

 2010-10-28T05:01:31.022+0000,I love strawberries,-,-
 2010-10-28T05:01:31.024+0000,I love strawberries,what a day!,-
 2010-10-28T05:01:31.027+0000,I love strawberries,what a day!,well done
 2010-10-28T05:01:31.032+0000,what a day!,I love strawberries,well done
 **/
public class Query2V2 extends Thread{
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

        Query2V2 query2 = new Query2V2(args);
        query2.run();
    }

    public Query2V2(String[] args){
        friendshipFile = args[0];
        postsFile = args[1];
        commentsFile = args[2];
        likesFile = args[3];
    }

    public void run(){
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@config(async = 'true')define stream eventsStream (iij_timestamp long, ts long, attribute_1 long, attribute_2 long, attribute_3 string, attribute_4 string, attribute_5 long, attribute_6 long, eventType int);";
        inStreamDefinition += "@config(async = 'true')define stream likesFriendshipsCommentsStream (iij_timestamp long, ts long, attribute_1 long, attribute_2 long, attribute_3 string, attribute_4 string, attribute_5 long, attribute_6 long, eventType int );";

        String query = ("@info(name = 'query1') from eventsStream " +
                "select iij_timestamp, ts, attribute_1, attribute_2, attribute_3, attribute_4, attribute_5, attribute_6, eventType " +
                "insert into likesFriendshipsCommentsStream;");

        query += ("@info(name = 'query4') from likesFriendshipsCommentsStream#debs2016:rankerQuery2(iij_timestamp, ts, attribute_1, attribute_2, attribute_3, attribute_4, attribute_5, attribute_6, eventType)  " +
                "select iij_timestamp " +
                "insert into query2OutputStream;");


        System.out.println(inStreamDefinition + query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);


        System.out.println("Incremental data loading is performed.");

        LinkedBlockingQueue<Object[]> eventBufferList [] = new LinkedBlockingQueue[3];
        InputHandler inputHandlerNew = executionPlanRuntime.getInputHandler("eventsStream");

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

        OrderedEventSenderThread orderedEventSenderThread = new OrderedEventSenderThread(eventBufferList, inputHandlerNew);

        executionPlanRuntime.start();

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
