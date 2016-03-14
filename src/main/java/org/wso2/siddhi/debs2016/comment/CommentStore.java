package org.wso2.siddhi.debs2016.comment;

import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by malithjayasinghe on 3/8/16.
 *
 * Stores a list of active comments (i.e. comments arrived < d time)
 */
public class CommentStore{

    private long duration;
    private TreeMap<Long,CommentLikeGraph> graph = new TreeMap<Long, CommentLikeGraph>();
    String [] previousKcomments ;
    private boolean debug = false;
    private long tsTriggeredChange;
    private Graph friendshipGraph;

    /**
     * The constructor
     *
     * @param d the duration
     */
    public  CommentStore(long d, Graph friendshipGraph){

        duration=d;
        this.friendshipGraph = friendshipGraph;
    }
    /**
     *
     * Updates the comment store based on the logical time of a new event
     *
     * @param time logical time of the new event
     */
    public void updateCommentStore(long time)
    {
        tsTriggeredChange = time;
        ArrayList<Long> keyListToDelete = new ArrayList<Long>();
        for (Long key: this.graph.keySet()) {
            long arrivalTime = this.graph.get(key).getArrivalTime();
            long lifetime = time -  arrivalTime;

            if(debug){

                System.out.println("comment_id = " +  graph.get(key).getComment() + ", time = " + time + ", arrival time = " + arrivalTime + ", lifeTime  = " + lifetime + ", Duration = " + duration);
            }

            if(duration < lifetime){
                keyListToDelete.add(key);
            }

        }
        for(int i = 0; i < keyListToDelete.size(); i++){
            graph.remove(keyListToDelete.get(i));
        }

    }

    /**
     * Print the comment store
     *
     * @param time current time
     */
    public void printCommentStore(Long time){

        System.out.println("number of comments "  + getNumberOfComments());
        for (Long key: graph.keySet()) {
            String comment = graph.get(key).getComment();
            Long arrivalTime = graph.get(key).getArrivalTime();
            Long lifeTime = (time - (Long) arrivalTime);
            System.out.println("   comment_id = " +  key  + ", comment = " + comment + ", arrival time = " + arrivalTime + ", lifeTime  = " + lifeTime + ", Remaining life= " + (duration - lifeTime));
        }

    }

    /**
     * print the k largest comments if there is change in the order
     *
     * @param k the number of comments
     */
    public void printKLargestComments(int k, String delimater)
    {
        if(getKLargestComments(k)){
            System.out.print(tsTriggeredChange );
            for (String print: previousKcomments) {
                System.out.print(delimater + print);
            }
            System.out.println();
        }
    }


    /**
     * print the k largest comments if there is change in the order
     *
     * @param k the number of comments
     */
    public void printKLargestComments(int k)
    {
       printKLargestComments(k, ",");
    }

    /**
     * Gets the k largest comments
     *
     * @param k the number of comments
     */
    private boolean getKLargestComments(int k)
    {
        ArrayList<String> commentsList = new ArrayList<String>();
        ArrayList<Long> list = new ArrayList<Long>();

        String [] kComments = new String[k];

        for (CommentLikeGraph eachCommentLikeGraph: graph.values()) {
            long sizeOfComponent = Graph.getLargestConnectedComponent(eachCommentLikeGraph.getGraph());
            String comment = eachCommentLikeGraph.getComment();

            /*If this is the first comment, add it to the list*/
            if (list.size() == 0){
                list.add(sizeOfComponent);
                commentsList.add(comment);
                continue;
            }

            /*If the element is the smallest, add it to the end*/
            if (sizeOfComponent < list.get(list.size()-1)){
                list.add(list.size(), sizeOfComponent);
                commentsList.add(commentsList.size(), comment);
                continue;
            } else if (sizeOfComponent == list.get(list.size()-1)){
                if (commentsList.get(commentsList.size()-1).compareTo(comment) <= 0) { //Lexicographical Ordering of last element
                    list.add(list.size(), sizeOfComponent);
                    commentsList.add(commentsList.size(), comment);
                    continue;
                    //TODO: Lexicographical Ordering
                }
            }

            /*Check each element to find correct position*/
            for (int i = 0; i < list.size(); i++) {
                if (sizeOfComponent == list.get(i)) {
                    if (commentsList.get(i).compareTo(comment) > 0) { //Lexicographical Ordering
                        list.add(i, sizeOfComponent);
                        commentsList.add(i, comment);
                        break;
                    }
                } else if (sizeOfComponent > list.get(i)) {
                    list.add(i, sizeOfComponent);
                    commentsList.add(i, comment);
                    break;
                }
            }
        }

        /*Check if a change has taken place in K largest comments*/
        boolean flagChange = false;

        if (list.size() == 0){

        }else{
            int limit = (k <= commentsList.size() ? k : commentsList.size());
            for (int i = 0; i < limit; i++){
                kComments[i] = commentsList.get(i);

                if (this.previousKcomments == null){
                    flagChange =  true;
                }else if (!(kComments[i].equals(this.previousKcomments[i]))) {
                    flagChange = true;
                }
            }

            if (limit == commentsList.size()){
                for (int i = commentsList.size(); i < k; i++){
                    kComments[i] = "-";
                }
            }

            if (flagChange){
                previousKcomments = kComments;
            }
        }
        return flagChange;
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
     * @printComment indicate whether to print the new comment details or not.
     */
    public void registerComment(long commentID, long ts, String comment, boolean printComment)
    {

        if(printComment)
        {
            System.out.println("new comment has arrived comment id " + commentID + ", arrival time " + ts + ", comment = " + comment);
        }

        if(!commentExists(commentID))
        {

            graph.put(commentID, new CommentLikeGraph(ts, comment, friendshipGraph));
        }
    }


    /**
     * Registers a comment in the comment store
     *
     * @param commentID the comment id
     * @param ts the arrival time of the comment
     *
     */
    public void registerComment(long commentID, long ts, String comment)
    {

        registerComment(commentID, ts, comment, false);
    }

    /**
     * Registers a like in the comment store
     *
     * @param userID the userID
     * @param commentID the comment id
     */
    public void registerLike(long commentID, long userID)
    {
        if (graph.get(commentID) != null){
            graph.get(commentID).registerLike(userID);
        }

    }
    /**
     * Handles a new friendship
     *
     * @param uId1 the userID of friend one
     * @param uId2 the userID of friend two
     */
    public void handleNewFriendship(long uId1, long uId2)
    {
        for (CommentLikeGraph commentLikeGraph: graph.values()) {
            commentLikeGraph.handleNewFriendship(uId1, uId2);
        }

    }

    /**
     * Get the number of comments in the comment store
     *
     * @return the number of comments
     */
    public long getNumberOfComments(){

        return graph.size();
    }

}
