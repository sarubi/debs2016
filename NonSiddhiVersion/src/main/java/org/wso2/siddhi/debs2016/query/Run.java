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

public class Run {

    public static void main(String[] args) {
        int y = Integer.parseInt(args[7]);
        int z;
        int f = Integer.parseInt(args[6]);

        if ((f < 3) && (f > -1) && (y > 1)) {
            if (args.length == 8) {
                if (f == 0) {
                    Query1.main(args);
                } else if ((f == 1)) {
                    Query2.main(args);
                } else if (f == 2) {
                    Query1.main(args);
                    Query2.main(args);
                }
            } else if (args.length == 9) {
                z = Integer.parseInt(args[8]);
                if ((z > 1)) {
                    Query1.main(args);
                    Query2.main(args);
                } else {
                    System.err.println("Incorrect arguments. Required:  Number of threads should be grater than one");
                }
            } else {
                System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat, value for k, value for d,Flag No , Number of threads > 1");
            }
        } else {
            System.err.println("Incorrect arguments. Required:  Required: Flag No. 0- Query1  1- Query2   2- Query1&Query2 , Number of threads should be grether than one");
        }
    }
}
