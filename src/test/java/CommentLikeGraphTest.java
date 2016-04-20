import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
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
        Multimap mp = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());
        commentLikeGraph.registerLike(31, mp);
        commentLikeGraph.registerLike(12, mp);
        commentLikeGraph.handleNewFriendship(12, 31, mp);

        Graph testGraph = new Graph();
        testGraph.addVertex(31);
        testGraph.addVertex(12);
        testGraph.addEdge(12, 31);

        assertEquals(testGraph.getNumberOfEdges(), commentLikeGraph.getGraph().getNumberOfEdges());
        assertEquals(testGraph.getNumberOfVertices(), commentLikeGraph.getGraph().getNumberOfVertices());

    }

    @Test
    public void testRegisterLike(){
        Multimap mp = TreeMultimap.create();
        CommentLikeGraph graph = new CommentLikeGraph("Hi", new Graph());
        assertEquals(0,graph.getGraph().getNumberOfVertices());
        graph.registerLike(31, mp);
        assertEquals(1,graph.getGraph().getNumberOfVertices());
        graph.registerLike(12, mp);
        assertEquals(2,graph.getGraph().getNumberOfVertices());
    }

    @Test
    public void testHandleFriendship(){
        Multimap mp = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());

        commentLikeGraph.registerLike(12, mp);
        commentLikeGraph.registerLike(15, mp);
        commentLikeGraph.registerLike(16, mp);

        commentLikeGraph.handleNewFriendship(15,16, mp);
        commentLikeGraph.handleNewFriendship(15,6, mp);
        commentLikeGraph.handleNewFriendship(2,9, mp);

        assertEquals(1, commentLikeGraph.getGraph().getNumberOfEdges());
    }
}
