/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ2;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

class Query2 {

    private static final int BUFFER_LIMIT = 100000;
    String friendshipFile;
    String commentsFile;
    String likesFile;
    int k;
    long duration;


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

    private Query2(String[] args){
        friendshipFile = args[0];
        commentsFile = args[2];
        likesFile = args[3];
        k = Integer.parseInt(args[4]);
        duration = Long.parseLong(args[5]);

    }

    /**
     * Starts the threads related to Query1
     */
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
        DataLoaderThread dataLoaderThreadFriendships = new DataLoaderThread(friendshipFile,FileType.FRIENDSHIPS,BUFFER_LIMIT);

        //Comments
        LinkedBlockingQueue<Object[]> eventBufferListComments = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(commentsFile, FileType.COMMENTS,BUFFER_LIMIT);

        //Likes
        LinkedBlockingQueue<Object[]> eventBufferListLikes = new LinkedBlockingQueue<Object[]>();
        DataLoaderThread dataLoaderThreadLikes = new DataLoaderThread(likesFile, FileType.LIKES,BUFFER_LIMIT);

        eventBufferList[0] = dataLoaderThreadFriendships.getEventBuffer();
        eventBufferList[1] = dataLoaderThreadComments.getEventBuffer();
        eventBufferList[2] = dataLoaderThreadLikes.getEventBuffer();

        OrderedEventSenderThreadQ2 orderedEventSenderThread = new OrderedEventSenderThreadQ2(eventBufferList, inputHandlerNew,k,duration);

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
