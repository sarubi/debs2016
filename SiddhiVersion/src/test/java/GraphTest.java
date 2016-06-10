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

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.junit.Test;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import static org.junit.Assert.*;

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
        Multimap<Long, String> multimap = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());

        commentLikeGraph.registerLike(100, multimap);
        commentLikeGraph.registerLike(101, multimap);
        commentLikeGraph.registerLike(102, multimap);
        commentLikeGraph.registerLike(103, multimap);
        commentLikeGraph.registerLike(104, multimap);
        commentLikeGraph.registerLike(105, multimap);
        commentLikeGraph.registerLike(106, multimap);
        commentLikeGraph.registerLike(107, multimap);
        commentLikeGraph.registerLike(108, multimap);
        commentLikeGraph.registerLike(109, multimap);

        commentLikeGraph.handleNewFriendship(100, 101, multimap);
        commentLikeGraph.handleNewFriendship(100, 105, multimap);
        commentLikeGraph.handleNewFriendship(105, 106, multimap);
        commentLikeGraph.handleNewFriendship(106, 103, multimap);
        commentLikeGraph.handleNewFriendship(107, 102, multimap);
        commentLikeGraph.handleNewFriendship(107, 104, multimap);
        commentLikeGraph.handleNewFriendship(102, 108, multimap);
        commentLikeGraph.handleNewFriendship(109, 108, multimap);


        assertEquals(5, commentLikeGraph.getGraph().getLargestConnectedComponent());
    }

    @Test
    public void testGetLargestConnectedComponentData2()
    {
        Multimap<Long, String> multimap = TreeMultimap.create();
        CommentLikeGraph commentLikeGraph = new CommentLikeGraph("Hi", new Graph());

        commentLikeGraph.registerLike(100, multimap);
        commentLikeGraph.registerLike(101, multimap);
        commentLikeGraph.registerLike(102, multimap);
        commentLikeGraph.registerLike(103, multimap);
        commentLikeGraph.registerLike(104, multimap);
        commentLikeGraph.registerLike(105, multimap);
        commentLikeGraph.registerLike(106, multimap);
        commentLikeGraph.registerLike(107, multimap);
        commentLikeGraph.registerLike(108, multimap);
        commentLikeGraph.registerLike(109, multimap);
        commentLikeGraph.registerLike(110, multimap);
        commentLikeGraph.registerLike(111, multimap);
        commentLikeGraph.registerLike(112, multimap);
        commentLikeGraph.registerLike(113, multimap);
        commentLikeGraph.registerLike(114, multimap);
        commentLikeGraph.registerLike(115, multimap);
        commentLikeGraph.registerLike(116, multimap);
        commentLikeGraph.registerLike(120, multimap);


        commentLikeGraph.handleNewFriendship(113, 103, multimap);
        commentLikeGraph.handleNewFriendship(101, 103, multimap);
        commentLikeGraph.handleNewFriendship(103, 114, multimap);
        commentLikeGraph.handleNewFriendship(102, 103, multimap);
        commentLikeGraph.handleNewFriendship(104, 102, multimap);
        commentLikeGraph.handleNewFriendship(109, 102, multimap);
        commentLikeGraph.handleNewFriendship(109, 110, multimap);
        commentLikeGraph.handleNewFriendship(105, 106, multimap);
        commentLikeGraph.handleNewFriendship(107, 106, multimap);
        commentLikeGraph.handleNewFriendship(107, 115, multimap);
        commentLikeGraph.handleNewFriendship(107, 111, multimap);
        commentLikeGraph.handleNewFriendship(107, 108, multimap);
        commentLikeGraph.handleNewFriendship(112, 108, multimap);
        commentLikeGraph.handleNewFriendship(116, 120, multimap);

        assertEquals(8, commentLikeGraph.getGraph().getLargestConnectedComponent());
    }
}
