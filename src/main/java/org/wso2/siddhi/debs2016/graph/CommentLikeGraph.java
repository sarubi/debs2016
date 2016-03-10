package org.wso2.siddhi.debs2016.graph;
import java.util.*;
/**
 * The graph of users who have liked a given comment where edges of the represents the friendship relationship between them.
 *
 * Created by malithjayasinghe on 3/9/16.
 */
public class CommentLikeGraph {

    private long ts;
    public Graph commentLikeGraph = new Graph();
	private String comment;

    /**
     *
     * The constructor
     *
     * @param ts the arrival time of the comment
     */
    public CommentLikeGraph(long ts, String comment)
    {
        this.ts = ts;
        this.comment = comment;

    }

    /**
     * Gets the arrival time of the comment
     *
     * @return the arrival time of the comment
     */
    public long getArrivalTime()
    {
        return ts;
    }

    /**
     * Register a new like for the comment
     *
     * @param uId is user id of person who likes comment
     */
    public void registerLike(long uId)
    {
        commentLikeGraph.addVertex(uId);
        Set<Long> verticesList = commentLikeGraph.getVerticesList();
        for (long vertex: verticesList) {

                if (Graph.friendshipGraph.hasEdge(uId, vertex)){
                    commentLikeGraph.addEdge(uId, vertex);
                }
        }
    }

    /**
     * Handle event of a new friendship in CommentLikeGraph
     *
     * @param uId1 the userID of friend one
     * @param uId2 the userID of friend two
     */
    public void handleNewFriendship(long uId1, long uId2) {
        if (commentLikeGraph.hasVertex(uId1) && commentLikeGraph.hasVertex(uId2)){
            commentLikeGraph.addEdge(uId1, uId2);
        }
    }
}
