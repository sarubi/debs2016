import org.junit.Test;
import org.wso2.siddhi.debs2016.comment.CommentStore;
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
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph(1,"Hi");

        commentLikeGraph.registerLike(100);
        commentLikeGraph.registerLike(101);
        commentLikeGraph.registerLike(102);
        commentLikeGraph.registerLike(103);
        commentLikeGraph.registerLike(104);
        commentLikeGraph.registerLike(105);
        commentLikeGraph.registerLike(106);
        commentLikeGraph.registerLike(107);
        commentLikeGraph.registerLike(108);
        commentLikeGraph.registerLike(109);

        commentLikeGraph.handleNewFriendship(100, 101);
        commentLikeGraph.handleNewFriendship(100, 105);
        commentLikeGraph.handleNewFriendship(105, 106);
        commentLikeGraph.handleNewFriendship(106, 103);
        commentLikeGraph.handleNewFriendship(107, 102);
        commentLikeGraph.handleNewFriendship(107, 104);
        commentLikeGraph.handleNewFriendship(102, 108);
        commentLikeGraph.handleNewFriendship(109, 108);

        assertEquals(5, graph.commentLikeGraph.getLargestConnectedComponent(graph.commentLikeGraph));
    }

    @Test
    public void testGetLargestConnectedComponentData2()
    {
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph(1,"Hi");

        commentLikeGraph.registerLike(100);
        commentLikeGraph.registerLike(101);
        commentLikeGraph.registerLike(102);
        commentLikeGraph.registerLike(103);
        commentLikeGraph.registerLike(104);
        commentLikeGraph.registerLike(105);
        commentLikeGraph.registerLike(106);
        commentLikeGraph.registerLike(107);
        commentLikeGraph.registerLike(108);
        commentLikeGraph.registerLike(109);
        commentLikeGraph.registerLike(110);
        commentLikeGraph.registerLike(111);
        commentLikeGraph.registerLike(112);
        commentLikeGraph.registerLike(113);
        commentLikeGraph.registerLike(114);
        commentLikeGraph.registerLike(115);
        commentLikeGraph.registerLike(116);
        commentLikeGraph.registerLike(120);


        commentLikeGraph.handleNewFriendship(113, 103);
        commentLikeGraph.handleNewFriendship(101, 103);
        commentLikeGraph.handleNewFriendship(103, 114);
        commentLikeGraph.handleNewFriendship(102, 103);
        commentLikeGraph.handleNewFriendship(104, 102);
        commentLikeGraph.handleNewFriendship(109, 102);
        commentLikeGraph.handleNewFriendship(109, 110);
        commentLikeGraph.handleNewFriendship(105, 106);
        commentLikeGraph.handleNewFriendship(107, 106);
        commentLikeGraph.handleNewFriendship(107, 115);
        commentLikeGraph.handleNewFriendship(107, 111);
        commentLikeGraph.handleNewFriendship(107, 108);
        commentLikeGraph.handleNewFriendship(112, 108);
        commentLikeGraph.handleNewFriendship(116, 120);

        assertEquals(8, Graph.getLargestConnectedComponent(commentLikeGraph.getGraph()));
    }

    @Test
    public void testFriendshipGraphLargestComponent(){
//        assertEquals(4139,Graph.friendshipGraph.getNumberOfVertices());
//        assertEquals(63409,Graph.friendshipGraph.getNumberOfEdges());
        assertEquals(4139,Graph.getLargestConnectedComponent(Graph.friendshipGraph));
    }

}
