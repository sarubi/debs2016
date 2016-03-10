package org.wso2.siddhi.debs2016.graph;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.List;

/**
 * Create New Graph
 * Created by anoukh on 3/7/16.
 */
public class Graph {

    private HashMap<Long, List<Long>> graph = new HashMap<Long, List<Long>>();
    public static Graph friendshipGraph = new Graph("/usr/wso2/DEBS/debs2016/src/main/resources/data/friendships.dat");

    /**
     * The constructor
     *
     * @param location the location of the file to create the graph from
     */
    public Graph(String location) { /*Constructor*/
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
    public Graph() {

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


    /**
     * Check if an edge is present in the graph
     *
     * @param uId1 the vertex 1
     * @param uId2 the vertex 2
     * @return true if edge is found else false
     */
    public boolean hasEdge(long uId1, long uId2){
        List<Long> adjacentVertices = graph.get(uId1);
        if (adjacentVertices == null){
            return false;
        }else{
            return adjacentVertices.contains(uId2);
        }
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
     * Get set of vertices
     *
     *
     * @return set of vertices
     */
    public Set<Long> getVerticesList(){
        return graph.keySet();
    }

    //TODO:  Implement logic to decide largest connected component

    /**
     * Gets the number of vertices of the largest connected component of the graph
     *
     * @return the largest connected components
     */
    public static long getLargestConnectedComponent(Graph graph){
        /*Creating the Pegasus Data Structure*/
        List<Long> list = new ArrayList<Long>(graph.getVerticesList());
        List<Component> componentList = new ArrayList<Component>();

        for (int i = 0; i < list.size(); i++) {
                componentList.add(new Component(list.get(i),(long)i));
        }

        int changes = 1;
        while(changes != 0){
            changes = 0;
            for(int k = 0;k < componentList.size();k++) {
                for (int j = 0; j < componentList.size(); j++) {
                    if (graph.hasEdge(componentList.get(k).getUId(), componentList.get(j).getUId())) {

                        if (componentList.get(k).getNId() > componentList.get(j).getNId()) {
                            componentList.get(k).setNId(componentList.get(j).getNId());
                            changes++;
                        } else if (componentList.get(k).getNId() < componentList.get(j).getNId()) {
                            componentList.get(j).setNId(componentList.get(k).getNId());
                            changes++;
                        }
                    }
                }
            }
        }
        /*End Creating the Pegasus Data Structure*/

        /*Calculate Largest Component*/
        long largeComponent = 0;
            int count;

            for (int m = 0; m < componentList.size(); m++){
                count = 1;
                for (int n = 0; n < componentList.size(); n++){
                    if (m == n){
                        continue;
                    }
                    if (componentList.get(m).getNId() == componentList.get(n).getNId()){
                        count++;
                    }
                }
                if (count > largeComponent){
                    largeComponent = count;
                }
            }

        return largeComponent;
        /*End Calculate Largest Component*/
    }
}
/*Component Object to hold UserID and NodeID*/
class Component{
    private long uId;
    private long nId;
    public void setUId(long uId){
        this.uId=uId;
    }
    public void setNId(long nId){
        this.nId=nId;
    }
    public long getUId(){
        return uId;
    }
    public long getNId(){
        return nId;
    }
    public Component(long userId,long nodeId){
        setUId(userId);
        setNId(nodeId);
    }
}

