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

import java.util.*;

public class Graph {

    private final HashMap<Long, List<Long>> graph = new HashMap<>();

    /**
     * The constructor
     */
    public Graph() {

    }

    /**
     * Prints the graph information
     *
     */
    public String toString()
    {
        return graph.toString() +
                "Vertices: " + getNumberOfVertices() +
                "Edges: " + getNumberOfEdges();
    }


    /**
     * Adds an edge to the graph
     *
     * @param userOneId user id 1
     * @param userTwoId user id 2
     */

    public void addEdge(long userOneId, long userTwoId){
        addVertex(userOneId);
        graph.get(userOneId).add(userTwoId);
        addVertex(userTwoId);
        graph.get(userTwoId).add(userOneId);
    }


    /**
     * Create new vertex if vertex not already present
     *
     * @param userId the user id
     *
     */
    public void addVertex(long userId){
        if (!hasVertex(userId)){
            graph.put(userId, new ArrayList<>());
        }
    }

    /**
     * Gets the number of edges
     *
     * @return the number of edges
     */
    public int getNumberOfEdges(){
        int numberOfEdges = 0;
        for (List<Long> value: graph.values()) {
            numberOfEdges += value.size();
        }
        return numberOfEdges/2; /*Because undirected, same edge will be calculated twice*/
    }

    /**
     * Gets the number of vertices
     *
     * @return the number of vertices
     */
    public int getNumberOfVertices(){
        return graph.size();
    }


    /**
     * Check if an edge is present in the graph
     *
     * @param userOneId the vertex 1
     * @param userTwoId the vertex 2
     * @return true if edge is found else false
     */
    public boolean hasEdge(long userOneId, long userTwoId){
        List<Long> adjacentVertices = graph.get(userOneId);
        return adjacentVertices != null && adjacentVertices.contains(userTwoId);
    }


    /**
     * Check if vertex is present in the graph
     *
     * @param userId the vertex
     * @return true if vertex is found else false
     */
    public boolean hasVertex(long userId){
        return graph.containsKey(userId);
    }


    /**
     * Gets set of vertices
     *
     * @return the vertices set
     */
    public Set<Long> getVerticesList(){
        return graph.keySet();
    }

    /**
     * Gets the number of vertices of the largest connected component of the graph
     *
     * @return the number of vertices in the largest connected component
     */
    public long getLargestConnectedComponent() {

        if (graph.size() == 0) {
            return 0L;
        }

        HashMap<Long, Long> pegasusMap = new HashMap<>();

        long i = 0;
        for (Long key : graph.keySet()) {
            pegasusMap.put(key, i);
            i++;
        }
            boolean changes;
            do{
                changes = false;
            for(Long userId: pegasusMap.keySet()) {
                for (Long referUserId: pegasusMap.keySet()) {
                    if (hasEdge(userId, referUserId)) {

                        if (pegasusMap.get(userId) > pegasusMap.get(referUserId)) {
                            pegasusMap.replace(userId, pegasusMap.get(referUserId));
                            changes = true;
                        } else if (pegasusMap.get(userId) < pegasusMap.get(referUserId)) {
                            pegasusMap.replace(referUserId, pegasusMap.get(userId));
                            changes = true;
                        }
                    }
                }
            }
        }while(changes);
        return calculateLargestComponent(pegasusMap);
    }

    /**
     * Calculates the size largest connected component
     * @param pegasusMap is the reference to the pegasusmap
     * @return size of largest connected component
     */
    private long calculateLargestComponent(HashMap<Long, Long> pegasusMap){
        long largeComponent = 0;
        for(Long nodeId: pegasusMap.values()){
            int count = 0;
            for(Long referNodeId: pegasusMap.values()){
                if (nodeId.equals(referNodeId)){
                    count++;
                }
            }
            if (count > largeComponent){
                largeComponent = count;
            }
        }
        return largeComponent;
    }
}


