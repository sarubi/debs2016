package org.wso2.siddhi.debs2016.graph;

import com.google.common.base.Splitter;
import org.graphstream.algorithm.Toolkit;
import org.graphstream.graph.implementations.SingleGraph;

import java.io.*;
import java.util.Iterator;

/**
 * Created by malithjayasinghe on 3/12/16.
 *
 * Anaylzes the friendship graph
 *
 */
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
