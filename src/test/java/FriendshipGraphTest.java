import org.junit.Test;
import org.wso2.siddhi.debs2016.graph.FriendshipGraph;

import static org.junit.Assert.*;

/**
 * Created by malithjayasinghe on 3/8/16.
 */
public class FriendshipGraphTest {


    @Test
    public void testGraphConstructionDataSet1()
    {
        FriendshipGraph graph = new FriendshipGraph("/usr/wso2/DEBS/data/friendships.dat");
        assertEquals(63409,graph.getNumberOfEdges());
        assertEquals(4139,graph.getNumberOfVertices());
    }

    public void testGraphConstructionDataSet2()
    {


    }

    @Test
    public void testAddEdge()
    {
        FriendshipGraph graph = new FriendshipGraph();
        assertEquals(0,graph.getNumberOfEdges());
        graph.addEdge(1,2);
        assertEquals(1,graph.getNumberOfEdges());
        graph.addEdge(1,5);
        assertEquals(2,graph.getNumberOfEdges());
    }

    @Test
    public void testAddVertex(){
        FriendshipGraph graph = new FriendshipGraph();
        assertEquals(0,graph.getNumberOfVertices());

        graph.addEdge(1,2); /*Adding 2 new vertices in the form of an edge*/
        assertEquals(2,graph.getNumberOfVertices());

        graph.addVertex(5); /*Adding a new vertex*/
        assertEquals(3,graph.getNumberOfVertices());

        graph.addEdge(5,7); /*Adding 1 new vertex in the form of an edge*/
        assertEquals(4,graph.getNumberOfVertices());

        graph.addVertex(2); /*Creating existing vertex*/
        assertEquals(4,graph.getNumberOfVertices());
    }

    public void testGetConnectedComponents()
    {


    }

    public void getLargestConnectedComponent()
    {


    }






}
