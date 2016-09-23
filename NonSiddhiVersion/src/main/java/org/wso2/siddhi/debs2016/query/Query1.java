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


import org.wso2.siddhi.debs2016.sender.Q1ThreadsWorkLoadDistributed;

import java.io.File;


class Query1 {

    /**
     * The main method
     *
     * @param args arguments
     */
    public static void main(String[] args) {

        File q1 = new File("q1.txt");
        q1.delete();

        File performance = new File("performance.txt");
        performance.delete();

        if (args.length == 0) {
            System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat, Number of threads");
            return;
        }
        new Query1(args);
    }

    /**
     * The constructor
     *
     * @param args arguments
     */
    private Query1(String[] args) {
        String postsFile = args[1];
        String commentsFile = args[2];
        String noOfThreads = args[7];
        int num = Integer.parseInt(noOfThreads);

        Q1ThreadsWorkLoadDistributed q1ThreadsWorkLoadDistributed = new Q1ThreadsWorkLoadDistributed(postsFile, commentsFile, num);
        q1ThreadsWorkLoadDistributed.startProcess();
    }

}
