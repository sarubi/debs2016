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
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ1;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

class Query1 {

    private static final int BUFFER_LIMIT = 1000;
    String postsFile;
    String commentsFile;

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
    private Query1(String[] args){
        postsFile = args[1];
        commentsFile = args[2];

    }

    /**
     * Starts the threads related to Query1
     */
    public void run() {
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream inStream (iij_timestamp long, ts long, user_id long, comment_id long, comment string, user_name string, comment_replied_id long, post_replied_id long, isPostFlag int);";
        inStreamDefinition += "@config(async = 'true')define stream postCommentsStream (iij_timestamp long, ts long, user_id long, comment_id long, comment string, user_name string, comment_replied_id long, post_replied_id long, isPostFlag int );";

        String query = ("@info(name = 'query1') from inStream " +
                "select iij_timestamp, ts, user_id, comment_id, comment, user_name, comment_replied_id, post_replied_id, isPostFlag " +
                "insert into postCommentsStream;");

        query += ("@info(name = 'query2') from postCommentsStream#debs2016:rankerQuery1(iij_timestamp, ts, user_id, comment_id, comment, user_name, comment_replied_id, post_replied_id, isPostFlag)  " +
                "select result " +
                "insert into query1OutputStream;");

        System.out.println(inStreamDefinition + query);
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        System.out.println("Incremental data loading is performed.");

        LinkedBlockingQueue<Object[]> eventBufferList[] = new LinkedBlockingQueue[2];
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inStream");


        //Posts
        DataLoaderThread dataLoaderThreadPosts = new DataLoaderThread(postsFile, FileType.POSTS, BUFFER_LIMIT);
//        InputHandler inputHandlerPosts = executionPlanRuntime.getInputHandler("postsStream");

        //Comments
        DataLoaderThread dataLoaderThreadComments = new DataLoaderThread(commentsFile, FileType.COMMENTS, BUFFER_LIMIT);
//        InputHandler inputHandlerComments = executionPlanRuntime.getInputHandler("commentsStream");


        eventBufferList[0] = dataLoaderThreadPosts.getEventBuffer();
        eventBufferList[1] = dataLoaderThreadComments.getEventBuffer();
//        inputHandler[0] = inputHandlerPosts;
//        inputHandler[1] = inputHandlerComments;

        //EventSenderThread senderThreadComments = new EventSenderThread(dataLoaderThreadComments.getEventBuffer(), inputHandlerComments, Integer.MAX_VALUE);
        OrderedEventSenderThreadQ1 orderedEventSenderThread = new OrderedEventSenderThreadQ1(eventBufferList, inputHandler);

        executionPlanRuntime.start();

        //start the data loading process
        dataLoaderThreadPosts.start();

        dataLoaderThreadComments.start();

        //from here onwards we start sending the events
        orderedEventSenderThread.start();

    }


}
