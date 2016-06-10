/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.debs2016.graph;

import com.google.common.collect.Multimap;

import java.util.*;

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
     * @param userId is user id of person who likes comment
     * @param componentSizeCommentMap is reference to Map that holds size and comment string of each comment
     */
    public void registerLike(long userId, Multimap<Long, String> componentSizeCommentMap) {
        graph.addVertex(userId);
        Set<Long> verticesList = graph.getVerticesList();
        for (long vertex: verticesList) {
            if (friendshipGraph.hasEdge(userId, vertex)){
                    graph.addEdge(userId, vertex);
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
     * @param userOneId the userID of friend one
     * @param userTwoId the userID of friend two
     * @param componentSizeCommentMap is reference to Map that holds size and comment string of each comment
     */
    public void handleNewFriendship(long userOneId, long userTwoId, Multimap<Long, String> componentSizeCommentMap) {
        if (graph.hasVertex(userOneId) && graph.hasVertex(userTwoId)) {
            graph.addEdge(userOneId, userTwoId);
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

    /**
     * Getter method for graph
     *
     * @return the graph
     */
    public Graph getGraph() {
        return graph;
    }
}
