import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.junit.Test;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import static org.junit.Assert.*;

public class CommentLikeGraphTest {

    @Test
    public void testCommentLikeGraphConstruction(){
        Multimap<Long, String> multimap = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());
        commentLikeGraph.registerLike(31, multimap);
        commentLikeGraph.registerLike(12, multimap);
        commentLikeGraph.handleNewFriendship(12, 31, multimap);

        Graph testGraph = new Graph();
        testGraph.addVertex(31);
        testGraph.addVertex(12);
        testGraph.addEdge(12, 31);

        assertEquals(testGraph.getNumberOfEdges(), commentLikeGraph.getGraph().getNumberOfEdges());
        assertEquals(testGraph.getNumberOfVertices(), commentLikeGraph.getGraph().getNumberOfVertices());

    }

    @Test
    public void testRegisterLike(){
        Multimap<Long, String> multimap = TreeMultimap.create();
        CommentLikeGraph graph = new CommentLikeGraph("Hi", new Graph());
        assertEquals(0,graph.getGraph().getNumberOfVertices());
        graph.registerLike(31, multimap);
        assertEquals(1,graph.getGraph().getNumberOfVertices());
        graph.registerLike(12, multimap);
        assertEquals(2,graph.getGraph().getNumberOfVertices());
    }

    @Test
    public void testHandleFriendship(){
        Multimap<Long, String> multimap = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());

        commentLikeGraph.registerLike(12, multimap);
        commentLikeGraph.registerLike(15, multimap);
        commentLikeGraph.registerLike(16, multimap);

        commentLikeGraph.handleNewFriendship(15,16, multimap);
        commentLikeGraph.handleNewFriendship(15,6, multimap);
        commentLikeGraph.handleNewFriendship(2,9, multimap);

        assertEquals(1, commentLikeGraph.getGraph().getNumberOfEdges());
    }
}
