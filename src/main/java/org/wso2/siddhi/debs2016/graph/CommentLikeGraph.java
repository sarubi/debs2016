package org.wso2.siddhi.debs2016.graph;

/**
 * The graph of users who have liked a given comment where edges of the represents the friendship relationship between them.
 *
 * Created by malithjayasinghe on 3/9/16.
 */
public class CommentLikeGraph {

    private long ts;
    private FriendshipGraph friendshipGraph = new FriendshipGraph();

    /**
     *
     * @param ts the arrival time of the comment
     */
    public CommentLikeGraph(long ts)
    {
        this.ts = ts;

    }

    /**
     * Gets the arrival time of the comment
     *
     * @return the arrival time of the comment
     */
    public long getArrivalTime()
    {
        return 1000;
    }



}
