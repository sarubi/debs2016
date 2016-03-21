package org.wso2.siddhi.debs2016.graph;
import java.util.*;
/**
 * The graph of users who have liked a given comment where edges of the represents the friendship relationship between them.
 *
 * Created by malithjayasinghe on 3/9/16.
 */
public class CommentLikeGraph {

    private long ts;
    private Graph graph = new Graph();
	private String comment;
    private long sizeOfLargestConnectedComponent;
    private Graph friendshipGraph ;
    private boolean dirty = true;


    /**
     *
     * The constructor
     *
     * @param ts the arrival time of the comment
     */
    public CommentLikeGraph(long ts, String comment, Graph friendshipGraph) {
        this.ts = ts;
        this.comment = comment;
        this.friendshipGraph = friendshipGraph;
    }

    /**
     * Gets the arrival time of the comment
     *
     * @return the arrival time of the comment
     */
    public long getArrivalTime() {
        return ts;
    }

    /**
     * Gets the size of the largest connected component
     *
     * @return the size of the largest connected component
     */
    public long getSizeOfLargestConnectedComponent()
    {
        if(dirty) {
            dirty = false;
            sizeOfLargestConnectedComponent = graph.getLargestConnectedComponent();

        }
        return sizeOfLargestConnectedComponent;
    }

    /**
     * Register a new like for the comment
     *
     * @param uId is user id of person who likes comment
     */
    public void registerLike(long uId) {
        graph.addVertex(uId);
        Set<Long> verticesList = graph.getVerticesList();
        for (long vertex: verticesList) {
            if (friendshipGraph.hasEdge(uId, vertex)){
                    graph.addEdge(uId, vertex);
            }
        }
        dirty = true;
    }


    /**
     * Handle event of a new friendship in CommentLikeGraph
     *
     * @param uId1 the userID of friend one
     * @param uId2 the userID of friend two
     */
    public void handleNewFriendship(long uId1, long uId2) {
        if (graph.hasVertex(uId1) && graph.hasVertex(uId2)){
            graph.addEdge(uId1, uId2);
            dirty = true;
        }
    }



    /**
     * Getter method for comment variable
     *
     * @return the comment
     */
    public String getComment() {

        return comment;

    }

    /**
     * Getter method for graph
     *
     * @return the graph
     */
    public Graph getGraph() {
        return graph;
    }
}
