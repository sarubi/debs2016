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

import org.wso2.siddhi.debs2016.sender.Q2ThreadsWorkLoadDistributed;

import java.io.File;

class Query2 {

    public static void main(String[] args) {

        File q2 = new File("q2.txt");
        q2.delete();

        if (args.length == 0) {
            System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat,  Number of threads");
            return;
        }

        new Query2(args);
    }

    private Query2(String[] args) {
        String friendshipFile = args[0];
        String commentsFile = args[2];
        String likesFile = args[3];
        int k = Integer.parseInt(args[4]);
        long d = Long.parseLong(args[5]);
        int noOfThreads;
        noOfThreads = Integer.parseInt(args[7]);
        if (args.length == 9) {
            noOfThreads = Integer.parseInt(args[8]);
        }

        Q2ThreadsWorkLoadDistributed q2ThreadsWorkLoadDistributed = new Q2ThreadsWorkLoadDistributed(friendshipFile, commentsFile, likesFile, d, k, noOfThreads);
        q2ThreadsWorkLoadDistributed.startProcess();
    }
}
