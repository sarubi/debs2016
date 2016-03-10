import org.junit.Test;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import static org.junit.Assert.*;

/**
 * Created by malithjayasinghe on 3/8/16.
 */
public class GraphTest {


    @Test
    public void testGraphConstructionDataSet1()
    {
        Graph graph = new Graph("/usr/wso2/DEBS/data/friendships.dat");
        assertEquals(63409,graph.getNumberOfEdges());
        assertEquals(4139,graph.getNumberOfVertices());
    }

    public void testGraphConstructionDataSet2()
    {


    }

    @Test
    public void testAddEdge()
    {
        Graph graph = new Graph();
        assertEquals(0,graph.getNumberOfEdges());
        graph.addEdge(1,2);
        assertEquals(1,graph.getNumberOfEdges());
        graph.addEdge(1,5);
        assertEquals(2,graph.getNumberOfEdges());
    }

    @Test
    public void testAddVertex(){
        Graph graph = new Graph();
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

    @Test
    public void testHasEdge(){
        Graph friendshipGraph = new Graph();
        friendshipGraph.addEdge(1,2);

        assertEquals(true,friendshipGraph.hasEdge(1,2));
        assertEquals(false,friendshipGraph.hasEdge(1,3));
        assertEquals(false,friendshipGraph.hasEdge(3,1));
        assertEquals(true,friendshipGraph.hasEdge(2,1));
    }

    @Test
    public void testFriendshipGraph(){
        assertEquals(4139, Graph.friendshipGraph.getNumberOfVertices());
    }

    public void testGetConnectedComponents()
    {


    }

    @Test
    public void testGetLargestConnectedComponentData1()
    {
        CommentLikeGraph graph = new CommentLikeGraph(1,"Hi");

        graph.registerLike(100);
        graph.registerLike(101);
        graph.registerLike(102);
        graph.registerLike(103);
        graph.registerLike(104);
        graph.registerLike(105);
        graph.registerLike(106);
        graph.registerLike(107);
        graph.registerLike(108);
        graph.registerLike(109);

        graph.handleNewFriendship(100, 101);
        graph.handleNewFriendship(100, 105);
        graph.handleNewFriendship(105, 106);
        graph.handleNewFriendship(106, 103);
        graph.handleNewFriendship(107, 102);
        graph.handleNewFriendship(107, 104);
        graph.handleNewFriendship(102, 108);
        graph.handleNewFriendship(109, 108);

        assertEquals(5, graph.commentLikeGraph.getLargestConnectedComponent(graph.commentLikeGraph));
    }

    @Test
    public void testGetLargestConnectedComponentData2()
    {
        CommentLikeGraph graph = new CommentLikeGraph(1,"Hi");

        graph.registerLike(100);
        graph.registerLike(101);
        graph.registerLike(102);
        graph.registerLike(103);
        graph.registerLike(104);
        graph.registerLike(105);
        graph.registerLike(106);
        graph.registerLike(107);
        graph.registerLike(108);
        graph.registerLike(109);
        graph.registerLike(110);
        graph.registerLike(111);
        graph.registerLike(112);
        graph.registerLike(113);
        graph.registerLike(114);
        graph.registerLike(115);
        graph.registerLike(116);
        graph.registerLike(120);


        graph.handleNewFriendship(113, 103);
        graph.handleNewFriendship(101, 103);
        graph.handleNewFriendship(103, 114);
        graph.handleNewFriendship(102, 103);
        graph.handleNewFriendship(104, 102);
        graph.handleNewFriendship(109, 102);
        graph.handleNewFriendship(109, 110);
        graph.handleNewFriendship(105, 106);
        graph.handleNewFriendship(107, 106);
        graph.handleNewFriendship(107, 115);
        graph.handleNewFriendship(107, 111);
        graph.handleNewFriendship(107, 108);
        graph.handleNewFriendship(112, 108);
        graph.handleNewFriendship(116, 120);

        assertEquals(8, graph.commentLikeGraph.getLargestConnectedComponent(graph.commentLikeGraph));
    }






}
