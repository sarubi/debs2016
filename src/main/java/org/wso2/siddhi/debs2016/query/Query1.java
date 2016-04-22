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

import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ1;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

class Query1 {
    private final OrderedEventSenderThreadQ1 orderedEventSenderThreadQ1;
    private final DataLoaderThread dataLoaderThreadComments ;
    private final DataLoaderThread dataLoaderThreadPosts;
    private static final int BUFFER_LIMIT = 1000;

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
        String postsFile = args[1];
        String commentsFile = args[2];

        LinkedBlockingQueue<Object[]> eventBufferListQ1 [] = new LinkedBlockingQueue[2];
        orderedEventSenderThreadQ1 = new OrderedEventSenderThreadQ1(eventBufferListQ1);
        dataLoaderThreadComments = new DataLoaderThread(commentsFile, FileType.COMMENTS, BUFFER_LIMIT);
        dataLoaderThreadPosts = new DataLoaderThread(postsFile, FileType.POSTS, BUFFER_LIMIT);
        eventBufferListQ1 [0] = dataLoaderThreadPosts.getEventBuffer();
        eventBufferListQ1 [1] = dataLoaderThreadComments.getEventBuffer();
    }

    /**
     * Starts the threads related to Query1
     */
    private void run(){
        dataLoaderThreadComments.start();
        dataLoaderThreadPosts.start();
        orderedEventSenderThreadQ1.start();
    }


}
