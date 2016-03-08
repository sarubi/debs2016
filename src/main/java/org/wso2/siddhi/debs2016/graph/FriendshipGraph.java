package org.wso2.siddhi.debs2016.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Create New FriendshipGraph
 * Created by anoukh on 3/7/16.
 */
public class FriendshipGraph {

    private HashMap<Long, List<Long>> graph = new HashMap<Long, List<Long>>();

    /**
     * The constructor
     *
     * @param location the location of the file to create the graph from
     */
    public FriendshipGraph(String location) { /*Constructor*/
        final String file = location;
        long uId1;
        long uId2;

        try {
            Scanner in = new Scanner(new File(file));
            in.useDelimiter("\\||\\n"); //Delimit using pipe symbol and return
            while (in.hasNext()) {
                in.next();  /*The Timestamp will not be saved for now*/
                uId1 = in.nextLong();
                uId2 = in.nextLong();
                addEdge(uId1, uId2);
            }
        }catch (IOException  e){
            e.printStackTrace();
        }
    }

    /**
     * The constructor
     */
    public FriendshipGraph() {

    }


    /**
     * Prints the graph information
     *
     */
    public void printGraphInfo()
    {
        System.out.println(graph.toString()); /*Print the Graph itself*/
        System.out.println("Vertices: " + getNumberOfVertices()); /*Print number of vertices*/
        System.out.println("Edges: " + getNumberOfEdges()); /*Print the number of edges*/

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
     * Check vertex
     *
     * @param uId the user id
     * @return true if vertex exists, false otherwise
     */

    private boolean checkVertex(long uId) { /*Check if vertex already present*/
        boolean flag = false;
        return graph.containsKey(uId) || flag;
    }

    /**
     * Create new vertex if vertex not already present
     *
     * @param uId the user id
     *
     */
    public void addVertex(long uId){
        if (!checkVertex(uId)){
            graph.put(uId, new ArrayList<Long>());
        }
    }

    /**
     * Gets the number of edges
     *
     * @return the number of edges
     */
    public int getNumberOfEdges(){ /*Calculate number of edges*/
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
    public int getNumberOfVertices(){
        return graph.size();
    }
    //TODO:  Implement logic to decide largest connected component

    /**
     * Gets the largest connect component of the graph
     *
     * @return the largest connected components
     */
    public FriendshipGraph getLargestConnectedComponent(){
        return null;
    }
}
