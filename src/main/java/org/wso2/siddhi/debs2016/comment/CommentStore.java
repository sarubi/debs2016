package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;

import java.util.HashMap;

/**
 * Created by malithjayasinghe on 3/8/16.
 *
 * Stores a list of active comments (i.e. comments arrived < d time)
 */
public class CommentStore {

   private HashMap<Long,CommentLikeGraph> graph = new HashMap<Long, CommentLikeGraph>();



    /**
     *
     * Updates the comment store based on the logical time of a new event
     *
     * @param time logical time of the new even
     */
    public void updateCommentStore(long time)
    {

            //for each comment check if it is publised more than d seconds ago. If so remove the comment from the hash map
            //get the new time stamp and the time stamp of the comment (where do we store this)

    }

    /**
     *
     * Deletes a comment with a given ID
     *
     * @param commentID of the comment to delete
     */

    private void deleteComment(long commentID)
    {
            // remove the item in the hash map where the comment id= commentID
    }

    /**
     * Gets the k largest comments
     *
     * @param k the number of comments
     */
    public String [] getKLargestComments(int k)
    {

        // go through the hash map map and get largest connected component of each like graph
        // the first k elements (note that we will only write this to the output steam if there has been a change in the output)
        return new String[0];
    }



    /**
     * Check if the vertex exists
     *
     * @param uId the user id
     * @return true if vertex exists, false otherwise
     */

    private boolean commentExists(long uId) { /*Check if vertex already present*/
        boolean flag = false;
        return graph.containsKey(uId) || flag;
    }


    /**
     * Registers a comment in the comment store
     *
     * @param commentID the comment id
     * @param ts the arrival time of the comment
     */
    public void registerComment(long commentID, long ts, String comment)
    {

        if(!commentExists(commentID))
        {
            graph.put(commentID, new CommentLikeGraph(ts, comment));
        }
    }

    /**
     * Registers a like in the comment store
     *
     * @param userID the userID
     * @param commentID the comment id
     */
    private void registerLike(long commentID, long userID)
    {
        graph.get(commentID).registerLike(userID);

    }
    /**
     * Handles a new friendship
     *
     * @param uId1 the userID of friend one
     * @param uId2 the userID of friend two
     */
    private void handleNewFriendship(long uId1, long uId2)
    {
        for (CommentLikeGraph commentLikeGraph: graph.values()) {
            commentLikeGraph.handleNewFriendship(uId1, uId2);
        }

    }

}
