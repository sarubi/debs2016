package org.wso2.siddhi.debs2016.graph;

import com.google.common.base.Splitter;
import org.graphstream.graph.implementations.SingleGraph;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by malithjayasinghe on 3/12/16.
 *
 * Anaylzes the friendship graph
 *
 */
public class GraphAnalyzer {

    org.graphstream.graph.Graph graph = new SingleGraph("Tutorial 1");
    private int updateRate;
    private int numOfEvents;
    /**
     * The constructor
     *
     * @param updateRate graph update rate
     */
    public GraphAnalyzer(int updateRate, int numOfEvents)
    {
        this.updateRate = updateRate;
        this.numOfEvents = numOfEvents;
    }

    /**
     * Display the friendship graph
     *
     */
    public void displayFriendshipGraph() {
        graph.display();
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

                if(updateRate>0)
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
                if(count == numOfEvents)
                {
                    break;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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



    public static void main(String args[])
    {
        GraphAnalyzer analyzer = new GraphAnalyzer(1000, 5);
        analyzer.displayFriendshipGraph();
    }


}
