package org.wso2.siddhi.debs2016.graph;

import com.google.common.collect.Multimap;

import java.util.*;
/**
 * The graph of users who have liked a given comment where edges of the represents the friendship relationship between them.
 *
 * Created by malithjayasinghe on 3/9/16.
 */
public class CommentLikeGraph {

    private final Graph graph = new Graph();
	private final String comment;
    private long sizeOfLargestConnectedComponent;
    private final Graph friendshipGraph ;


    /**
     *
     * The constructor
     *
     * @param comment is the comment string
     * @param friendshipGraph is the reference to the friendshipGraph
     */
    public CommentLikeGraph(String comment, Graph friendshipGraph) {
        this.comment = comment;
        this.friendshipGraph = friendshipGraph;
        this.sizeOfLargestConnectedComponent = 0;
    }

    /**
     * Gets the size of the largest connected component by running pegasus
     *
     * @return the size of the largest connected component
     */
    private long computeLargestConnectedComponent()
    {
        sizeOfLargestConnectedComponent = graph.getLargestConnectedComponent();
        return sizeOfLargestConnectedComponent;
    }

    /**
     * Register a new like for the comment
     *
     * @param uId is user id of person who likes comment
     * @param componentSizeCommentMap is reference to Map that holds size and comment string of each comment
     */
    public void registerLike(long uId, Multimap componentSizeCommentMap) {
        graph.addVertex(uId);
        Set<Long> verticesList = graph.getVerticesList();
        for (long vertex: verticesList) {
            if (friendshipGraph.hasEdge(uId, vertex)){
                    graph.addEdge(uId, vertex);
            }
        }
        if (sizeOfLargestConnectedComponent != 0){
            componentSizeCommentMap.remove(getSizeOfLargestConnectedComponent(), comment);
        }
        sizeOfLargestConnectedComponent = computeLargestConnectedComponent();
        componentSizeCommentMap.put(sizeOfLargestConnectedComponent, comment);
    }


    /**
     * Handle event of a new friendship in CommentLikeGraph
     *
     * @param uId1 the userID of friend one
     * @param uId2 the userID of friend two
     * @param componentSizeCommentMap is reference to Map that holds size and comment string of each comment
     */
    public void handleNewFriendship(long uId1, long uId2, Multimap componentSizeCommentMap) {
        if (graph.hasVertex(uId1) && graph.hasVertex(uId2)) {
            graph.addEdge(uId1, uId2);
            if (sizeOfLargestConnectedComponent != 0){
                componentSizeCommentMap.remove(getSizeOfLargestConnectedComponent(), comment);
            }
            sizeOfLargestConnectedComponent = computeLargestConnectedComponent();
            componentSizeCommentMap.put(sizeOfLargestConnectedComponent, comment);
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
     *
     * Get the size of the largest connected component
     *
     * @return size of largest connected component
     */
    public long getSizeOfLargestConnectedComponent() {
        return sizeOfLargestConnectedComponent;
    }
}
