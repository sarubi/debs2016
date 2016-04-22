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
import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ2;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

class Query2 {
    private final DataLoaderThread dataLoaderThreadFriendships;
    private final DataLoaderThread dataLoaderThreadComments;
    private final DataLoaderThread dataLoaderThreadLikes;
    private final OrderedEventSenderThreadQ2 orderedEventSenderThreadQ2;
    private static final int BUFFER_LIMIT = 100000;


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
        String friendshipFile = args[0];
        String commentsFile = args[2];
        String likesFile = args[3];
        int k = Integer.parseInt(args[4]);
        long d = Long.parseLong(args[5]);

        LinkedBlockingQueue<Object[]> eventBufferListQ2 [] = new LinkedBlockingQueue[3];
        dataLoaderThreadFriendships = new DataLoaderThread(friendshipFile, FileType.FRIENDSHIPS, BUFFER_LIMIT);
        dataLoaderThreadComments = new DataLoaderThread(commentsFile, FileType.COMMENTS, BUFFER_LIMIT);
        dataLoaderThreadLikes = new DataLoaderThread(likesFile, FileType.LIKES, BUFFER_LIMIT);
        eventBufferListQ2[0] = dataLoaderThreadFriendships.getEventBuffer();
        eventBufferListQ2[1] = dataLoaderThreadComments.getEventBuffer();
        eventBufferListQ2[2] = dataLoaderThreadLikes.getEventBuffer();
        orderedEventSenderThreadQ2 = new OrderedEventSenderThreadQ2(eventBufferListQ2, k, d);

    }

    /**
     * Starts the threads related to Query1
     */
    private void run(){
        dataLoaderThreadFriendships.start();
        dataLoaderThreadComments.start();
        dataLoaderThreadLikes.start();
        orderedEventSenderThreadQ2.start();
    }
}
