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

package org.wso2.siddhi.debs2016.graph;

import com.google.common.base.Splitter;
import org.graphstream.algorithm.Toolkit;
import org.graphstream.graph.implementations.SingleGraph;

import java.io.*;
import java.util.Iterator;

class GraphAnalyzer {

    private final org.graphstream.graph.Graph graph = new SingleGraph("Tutorial 1");

    /**
     * Displays the friendship graph
     *
     * @param displayWhileLoading true will display while stream is processing
     * @param numberOfEventsToLoad will limit the number of events to load
     * @param updateRate speed at which graph grows
     */
    private void loadFriendshipGraph(boolean displayWhileLoading, int numberOfEventsToLoad, int updateRate) {

        if(displayWhileLoading) {
            graph.display();
        }

        int count = 0;

        try {
            Splitter splitter = Splitter.on('|');
            BufferedReader br = new BufferedReader(new FileReader("/Users/malithjayasinghe/debs2016/DataSet/data" + "/friendships.dat"), 10 * 1024 * 1024);
            String line = br.readLine();
            while (line != null) {
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                Long user1ID = Long.parseLong(dataStrIterator.next());
                Long user2ID = Long.parseLong(dataStrIterator.next());

                if(updateRate > 0)
                {
                    try {
                        Thread.sleep(updateRate);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                addEdge(user1ID, user2ID);
                line =  br.readLine();
                count++;

                if(count == numberOfEventsToLoad)
                {
                    break;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Add edge to the friendshipGraph
     *
     * @param user1ID the id of user 1
     * @param user2ID the id of user 2
     */
    private void addEdge(Long user1ID, Long user2ID )
    {
        if(graph.getNode(user1ID.toString()) == null){
            graph.addNode(user1ID.toString());
        }
        if(graph.getNode(user2ID.toString()) == null) {
            graph.addNode(user2ID.toString());
        }

        graph.addEdge(user1ID.toString()+user2ID.toString(), user1ID.toString(), user2ID.toString());
    }

    /**
     *
     * Gets the friendshipgraph
     *
     * @return the friendshipgraph
     */
    private org.graphstream.graph.Graph getFriendshipGraph()
    {
        return graph;
    }



    public static void main(String args[])
    {
        GraphAnalyzer analyzer = new GraphAnalyzer();
        analyzer.loadFriendshipGraph(true, 1000, 0);
        int degreeDistribution [] = Toolkit.degreeDistribution(analyzer.getFriendshipGraph());
        try {
            PrintWriter writer = new PrintWriter("/Users/malithjayasinghe/debs2016/DataSet/data" + "/degreeDistribution.csv", "UTF-8");

            int numElements = degreeDistribution.length;
            for(int i = 0; i <numElements; i++) {
                writer.println(i + "," + degreeDistribution[i] );
            }
            writer.close();

        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            e.printStackTrace();

        }
    }


}
