import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.junit.Test;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import static org.junit.Assert.*;

/**
 * Created by malithjayasinghe on 3/8/16.
 */
public class GraphTest {


    @Test
    public void testConstruction(){
        Graph graph = new Graph();
        assertEquals(0,graph.getNumberOfVertices());
        assertEquals(0,graph.getNumberOfEdges());
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
    public void testGetLargestConnectedComponentData1()
    {
        Multimap mp = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());

        commentLikeGraph.registerLike(100, mp);
        commentLikeGraph.registerLike(101, mp);
        commentLikeGraph.registerLike(102, mp);
        commentLikeGraph.registerLike(103, mp);
        commentLikeGraph.registerLike(104, mp);
        commentLikeGraph.registerLike(105, mp);
        commentLikeGraph.registerLike(106, mp);
        commentLikeGraph.registerLike(107, mp);
        commentLikeGraph.registerLike(108, mp);
        commentLikeGraph.registerLike(109, mp);

        commentLikeGraph.handleNewFriendship(100, 101, mp);
        commentLikeGraph.handleNewFriendship(100, 105, mp);
        commentLikeGraph.handleNewFriendship(105, 106, mp);
        commentLikeGraph.handleNewFriendship(106, 103, mp);
        commentLikeGraph.handleNewFriendship(107, 102, mp);
        commentLikeGraph.handleNewFriendship(107, 104, mp);
        commentLikeGraph.handleNewFriendship(102, 108, mp);
        commentLikeGraph.handleNewFriendship(109, 108, mp);


        assertEquals(5, commentLikeGraph.getGraph().getLargestConnectedComponent());
    }

    @Test
    public void testGetLargestConnectedComponentData2()
    {
        Multimap mp = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());

        commentLikeGraph.registerLike(100, mp);
        commentLikeGraph.registerLike(101, mp);
        commentLikeGraph.registerLike(102, mp);
        commentLikeGraph.registerLike(103, mp);
        commentLikeGraph.registerLike(104, mp);
        commentLikeGraph.registerLike(105, mp);
        commentLikeGraph.registerLike(106, mp);
        commentLikeGraph.registerLike(107, mp);
        commentLikeGraph.registerLike(108, mp);
        commentLikeGraph.registerLike(109, mp);
        commentLikeGraph.registerLike(110, mp);
        commentLikeGraph.registerLike(111, mp);
        commentLikeGraph.registerLike(112, mp);
        commentLikeGraph.registerLike(113, mp);
        commentLikeGraph.registerLike(114, mp);
        commentLikeGraph.registerLike(115, mp);
        commentLikeGraph.registerLike(116, mp);
        commentLikeGraph.registerLike(120, mp);


        commentLikeGraph.handleNewFriendship(113, 103, mp);
        commentLikeGraph.handleNewFriendship(101, 103, mp);
        commentLikeGraph.handleNewFriendship(103, 114, mp);
        commentLikeGraph.handleNewFriendship(102, 103, mp);
        commentLikeGraph.handleNewFriendship(104, 102, mp);
        commentLikeGraph.handleNewFriendship(109, 102, mp);
        commentLikeGraph.handleNewFriendship(109, 110, mp);
        commentLikeGraph.handleNewFriendship(105, 106, mp);
        commentLikeGraph.handleNewFriendship(107, 106, mp);
        commentLikeGraph.handleNewFriendship(107, 115, mp);
        commentLikeGraph.handleNewFriendship(107, 111, mp);
        commentLikeGraph.handleNewFriendship(107, 108, mp);
        commentLikeGraph.handleNewFriendship(112, 108, mp);
        commentLikeGraph.handleNewFriendship(116, 120, mp);

        assertEquals(8, commentLikeGraph.getGraph().getLargestConnectedComponent());
    }
}
