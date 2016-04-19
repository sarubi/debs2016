package org.wso2.siddhi.debs2016.graph;

import java.util.*;

/**
 * Create New Graph
 * Created by anoukh on 3/7/16.
 */
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
     * @param uId1 user id 1
     * @param uId2 user id 2
     */

    public void addEdge(long uId1, long uId2){
        addVertex(uId1);
        graph.get(uId1).add(uId2);
        addVertex(uId2);
        graph.get(uId2).add(uId1);
    }


    /**
     * Create new vertex if vertex not already present
     *
     * @param uId the user id
     *
     */
    public void addVertex(long uId){
        if (!hasVertex(uId)){
            graph.put(uId, new ArrayList<>());
        }
    }

    /**
     * Gets the number of edges
     *
     * @return the number of edges
     */
    private int getNumberOfEdges(){
        int numberOfEdges = 0;
        for (List<Long> val: graph.values()) {
            numberOfEdges += val.size();
        }
        return numberOfEdges/2; /*Because undirected, same edge will be calculated twice*/
    }

    /**
     * Gets the number of vertices
     *
     * @return the number of vertices
     */
    private int getNumberOfVertices(){
        return graph.size();
    }


    /**
     * Check if an edge is present in the graph
     *
     * @param uId1 the vertex 1
     * @param uId2 the vertex 2
     * @return true if edge is found else false
     */
    public boolean hasEdge(long uId1, long uId2){
        List<Long> adjacentVertices = graph.get(uId1);
        return adjacentVertices != null && adjacentVertices.contains(uId2);
    }


    /**
     * Check if vertex is present in the graph
     *
     * @param uId the vertex
     * @return true if vertex is found else false
     */
    public boolean hasVertex(long uId){
        return graph.containsKey(uId);
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

            int changes = 1;
            while(changes != 0){
                changes = 0;
            for(Long userId: pegasusMap.keySet()) {
                for (Long referUserId: pegasusMap.keySet()) {
                    if (hasEdge(userId, referUserId)) {

                        if (pegasusMap.get(userId) > pegasusMap.get(referUserId)) {
                            pegasusMap.replace(userId, pegasusMap.get(referUserId));
                            changes++;
                        } else if (pegasusMap.get(userId) < pegasusMap.get(referUserId)) {
                            pegasusMap.replace(referUserId, pegasusMap.get(userId));
                            changes++;
                        }
                    }
                }
            }
        }
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
                if (nodeId == referNodeId){
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


