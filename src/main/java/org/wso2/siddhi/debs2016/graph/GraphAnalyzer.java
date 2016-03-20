package org.wso2.siddhi.debs2016.graph;

import com.google.common.base.Splitter;
import org.graphstream.algorithm.Toolkit;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.util.Iterator;

/**
 * Created by malithjayasinghe on 3/12/16.
 *
 * Anaylzes the friendship graph
 *
 */
public class GraphAnalyzer {

    org.graphstream.graph.Graph graph = new SingleGraph("Tutorial 1");

    /**
     * Displays the friendship graph
     *
     */
    public void loadFriendshipGraph(boolean displayWhileLoading, int numberOfEventsToLoad, int updateRate) {

        if(displayWhileLoading) {
            graph.display();
        }

        int count = 0;

        try {
            Splitter splitter = Splitter.on('|');
            BufferedReader br = new BufferedReader(new FileReader("/Users/malithjayasinghe/debs2016/DataSet/data" + "/friendships.dat"), 10 * 1024 * 1024);
            String line = br.readLine();
            Object[] eventData;
            String user;
            while (line != null) {
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String friendshipsTimeStamp = dataStrIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                DateTime dt3 = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStamp);
                Long friendshipTimeStampLong = dt3.getMillis();
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

    public void loadFriendshipGraph() {
        loadFriendshipGraph(false, 0, 0);
    }


    /**
     * Display the friendshipgraph
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
    public org.graphstream.graph.Graph getFriendshipGraph()
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

        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }


}
