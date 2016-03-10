import org.junit.Test;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import static org.junit.Assert.*;

/**
 * Created by anoukh on 3/10/16.
 */
public class CommentLikeGraphTest {

    @Test
    public void testCommentLikeGraphConstruction(){
        CommentLikeGraph graph = new CommentLikeGraph(1, "Hi");
        graph.registerLike(31);
        graph.registerLike(12);
        graph.handleNewFriendship(12, 31);

        Graph testGraph = new Graph();
        testGraph.addVertex(31);
        testGraph.addVertex(12);
        testGraph.addEdge(12,31);

        assertTrue(testGraph.equals(graph.commentLikeGraph));

    }

    @Test
    public void testRegisterLike(){
        CommentLikeGraph graph = new CommentLikeGraph(1, "Hi");
        assertEquals(0,graph.commentLikeGraph.getNumberOfVertices());
        graph.registerLike(31);
        assertEquals(1,graph.commentLikeGraph.getNumberOfVertices());
        graph.registerLike(12);
        assertEquals(2,graph.commentLikeGraph.getNumberOfVertices());
    }

    @Test
    public void testHandleFriendship(){
        CommentLikeGraph graph = new CommentLikeGraph(1, "Hi");

        graph.registerLike(12);
        graph.registerLike(15);
        graph.registerLike(16);

        graph.handleNewFriendship(15,16);
        graph.handleNewFriendship(15,6);
        graph.handleNewFriendship(2,9);

        assertEquals(1, graph.commentLikeGraph.getNumberOfEdges());
    }
}
