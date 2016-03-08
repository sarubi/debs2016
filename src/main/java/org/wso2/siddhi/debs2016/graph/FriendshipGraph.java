package org.wso2.siddhi.debs2016.graph;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Created by anoukh on 3/7/16.
 */
public class FriendshipGraph { /*Create New FriendshipGraph object by passing in location of .dat file*/

    private HashMap<Long, List<Long>> graph = new HashMap<Long, List<Long>>();

    public FriendshipGraph(String location) { /*Constructor*/
        try {
            createGraph(location);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createGraph(String location) throws IOException{   /*Load file from disk*/
        final String file = location;
        long uId1;
        long uId2;

        Scanner in = new Scanner(new File(file));
        in.useDelimiter("\\||\\n"); //Delimit using pipe symbol and return

        while (in.hasNext()){
            in.next();  /*The Timestamp will not be saved for now*/
            uId1 = in.nextLong();
            uId2 = in.nextLong();
            addEdge(uId1,uId2);
        }

        System.out.println(graph.toString()); /*Print the Graph itself*/
        System.out.println(graph.size()); /*Print number of vertices*/
        System.out.println(getNumberOfEdges()); /*Print the Graph itself*/
    }

    private void addEdge(long uId1, long uId2){
        if(checkVertex(uId1)){
            graph.get(uId1).add(uId2);  /*If vertex already present create edge*/
        }else{
            graph.put(uId1, new ArrayList<Long>()); /*If vertex not present, create it*/
            graph.get(uId1).add(uId2); /*Create Edge*/
        }
        /*Same procedure for second vertex because it is undirected graph*/
        if(checkVertex(uId2)){
            graph.get(uId2).add(uId1);
        }else{
            graph.put(uId2, new ArrayList<Long>());
            graph.get(uId2).add(uId1);
        }
    }

    private boolean checkVertex(long uId) { /*Check if vertex already present*/
        boolean flag = false;
        return graph.containsKey(uId) || flag;
    }

    private int getNumberOfEdges(){ /*Calculate number of edges*/
        int numberOfEdges = 0;
        for (List<Long> val: graph.values()) {
            numberOfEdges += val.size();
        }
        return numberOfEdges/2; /*Because undirected, same edge will be calculated twice*/
    }

    //TODO:  Implement logic to decide largest connected component
    private int getLargestConnectedComponent(){
        return 0;
    }
}
