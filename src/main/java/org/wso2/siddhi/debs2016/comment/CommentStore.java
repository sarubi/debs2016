package org.wso2.siddhi.debs2016.comment;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import java.io.*;
import java.util.*;

/**
 * Created by malithjayasinghe on 3/8/16.
 * <p/>
 * Stores a list of active comments (i.e. comments arrived < d time)
 */
public class CommentStore {

    private long duration;
    private HashMap<Long, CommentLikeGraph> commentStore = new HashMap<Long, CommentLikeGraph>();
    String[] previousKcomments;
    private boolean debug = false;
    private long tsTriggeredChange;
    private Graph friendshipGraph;
    private ArrayList<String> commentsList = new ArrayList<String>();
    private ArrayList<Long> list = new ArrayList<Long>();
    private String[] kComments;
    private int k ;
    private File q2 ;
    StringBuilder builder = new StringBuilder();
    BufferedWriter writer;
    Multimap<Long, String> componentSizeCommentMap = TreeMultimap.create(Comparator.<Long>reverseOrder(), Comparator.<String>naturalOrder());



    /**
     * The constructor
     *
     * @param duration the duration
     */
    public CommentStore(long duration, Graph friendshipGraph, int k) {
        System.out.println("Query 2: version 2");
        this.duration = duration;
        this.friendshipGraph = friendshipGraph;
        this.k = k;
        kComments = new String[k];
        previousKcomments = new String[k];
        q2 = new File("q2.txt");
        try{
            writer = new BufferedWriter(new FileWriter(q2, true));
        }catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Updates the comment store based on the logical time of a new event
     *
     * @param time logical time of the new event
     */
    public void cleanCommentStore(long time) {
        tsTriggeredChange = time;

        for(Iterator<Map.Entry<Long, CommentLikeGraph>> it = this.commentStore.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Long, CommentLikeGraph> entry = it.next();
            long key = entry.getKey();
            long arrivalTime = this.commentStore.get(key).getArrivalTime();
            long lifetime = time - arrivalTime;


            if (duration < lifetime) {
                it.remove();
            }

        }
//
    }

    /**
     * Print the comment store
     *
     * @param time current time
     */
    public void printCommentStore(Long time) {

        System.out.println("number of comments " + getNumberOfComments());
        for (Long key : commentStore.keySet()) {
            String comment = commentStore.get(key).getComment();
            Long arrivalTime = commentStore.get(key).getArrivalTime();
            Long lifeTime = (time - (Long) arrivalTime);
            System.out.println("   comment_id = " + key + ", comment = " + comment + ", arrival time = " + arrivalTime + ", lifeTime  = " + lifeTime + ", Remaining life= " + (duration - lifeTime));
        }

    }


    /**
     * print the k largest comments if there is change in the order
     *

     * @param delimiter the delimiter to printed in between outputs
     * @param printKComments true would print in terminal. False will not print in terminal
     */
    public long computeKLargestComments(String delimiter, boolean printKComments, boolean writeToFile) {

            updateKLargestComments();
            try {
                if (hasKLargestCommentsChanged()) {
                    builder.setLength(0);
                    builder.append(tsTriggeredChange);
                    for (String print : previousKcomments) {
                        builder.append(delimiter + print);
                    }
                    builder.append("\n");
                    if (printKComments) {
                        System.out.println(builder.toString());
                    }

                    if (writeToFile) {
                        writer.write(builder.toString());
                    }
                    return System.currentTimeMillis();
                }
            }catch(IOException e)
            {
                e.printStackTrace();
            }
        return  -1L;
    }

    /**
     *
     * De-allocate resources
     *
     */
    public void destroy(){

        try {
            writer.close();
        }catch (IOException e)
        {
            e.printStackTrace();
        }
    }


//Old Implementation Has been commented
//    /**
//     * Update the K Largest comment arrays
//     *
//
//     */
//
//    private void updateKLargestComments() {
//
//        list.clear();
//        commentsList.clear();
//        for (CommentLikeGraph commentLikeGraph : commentStore.values()) {
//            long sizeOfComponent = commentLikeGraph.getSizeOfLargestConnectedComponent();
//            if (sizeOfComponent == 0 ){
//                continue;
//            }
//            String comment = commentLikeGraph.getComment();
//
//            /*If this is the first comment, add it to the list*/
//            if (list.size() == 0) {
//                list.add(sizeOfComponent);
//                commentsList.add(comment);
//                continue;
//            }
//
//            /*If the element is the smallest, add it to the end*/
//            if (sizeOfComponent < list.get(list.size() - 1)) {
//                list.add(list.size(), sizeOfComponent);
//                commentsList.add(commentsList.size(), comment);
//                continue;
//            } else if (sizeOfComponent == list.get(list.size() - 1)) {
//                if (commentsList.get(commentsList.size() - 1).compareTo(comment) <= 0) { //Lexicographical Ordering of last element
//                    list.add(list.size(), sizeOfComponent);
//                    commentsList.add(commentsList.size(), comment);
//                    continue;
//                }
//            }
//
//            /*Check each element to find correct position*/
//            for (int i = 0; i < list.size(); i++) {
//                if (sizeOfComponent == list.get(i)) {
//                    if (commentsList.get(i).compareTo(comment) > 0) { //Lexicographical Ordering
//                        list.add(i, sizeOfComponent);
//                        commentsList.add(i, comment);
//                        break;
//                    }
//                } else if (sizeOfComponent > list.get(i)) {
//                    list.add(i, sizeOfComponent);
//                    commentsList.add(i, comment);
//                    break;
//                }
//            }
//        }
//
//    }
//
//    /**
//     * Check if the k largest comments have changed
//     *
//     * @return true if if it has changed false otherwise
//     */
//    private boolean hasKLargestCommentsChanged()
//    {
//         /*Check if a change has taken place in K largest comments*/
//        boolean debug = true;
//
//        boolean flagChange = false;
//        kComments = new String[k];
//        if (list.size() != 0) {
//            int limit = (k <= commentsList.size() ? k : commentsList.size());
//            for (int i = 0; i < limit; i++) {
//                kComments[i] = commentsList.get(i);
//
//                if (previousKcomments == null) {
//                    flagChange = true;
//                } else if (!(kComments[i].equals(previousKcomments[i]))) {
//                    flagChange = true;
//                }
//            }
//
//            if (limit == commentsList.size()) {
//                for (int i = commentsList.size(); i < k; i++) {
//                    kComments[i] = "-";
//                }
//            }
//
//            if (flagChange) {
//                previousKcomments = kComments;
//            }
//        }
//        return flagChange;
//    }

    /**
     * Update the K Largest comment arrays
     *

     */
    private void updateKLargestComments() {
        componentSizeCommentMap.clear();
        for (CommentLikeGraph commentLikeGraph : commentStore.values()) {
            long sizeOfComponent = commentLikeGraph.getSizeOfLargestConnectedComponent();
            if (sizeOfComponent == 0){
                continue;
            }
            String comment = commentLikeGraph.getComment();

            componentSizeCommentMap.put(sizeOfComponent, comment);
        }
    }

    /**
     * Check if the k largest comments have changed
     *
     * @return true if if it has changed false otherwise
     */
    private boolean hasKLargestCommentsChanged()
    {
         /*Check if a change has taken place in K largest comments*/
        boolean debug = true;

        boolean flagChange = false;
        kComments = new String[k];
        if (componentSizeCommentMap != null) {
            int limit = (k <= componentSizeCommentMap.size() ? k : componentSizeCommentMap.size());
            int i = 0;

            for (String comment: componentSizeCommentMap.values()){
                kComments[i] = comment;
                if (previousKcomments == null) {
                    flagChange = true;
                } else if (!(kComments[i].equals(previousKcomments[i]))) {
                    flagChange = true;
                }
                i++;
                if (i == limit){
                    break;
                }
            }
            if (limit == componentSizeCommentMap.size()) {
                for (int j = componentSizeCommentMap.size(); j < k; j++) {
                    kComments[j] = "-";
                }
            }

            if (flagChange) {
                previousKcomments = kComments;
            }
        }
        return flagChange;
    }

    


    /**
     * Registers a comment in the comment store
     *
     * @param commentID the comment id
     * @param ts        the arrival time of the comment
     * @printComment indicate whether to print the new comment details or not.
     */
    public void registerComment(long commentID, long ts, String comment, boolean printComment) {

        if (printComment) {
            System.out.println("new comment has arrived comment id " + commentID + ", arrival time " + ts + ", comment = " + comment);
        }

        commentStore.put(commentID, new CommentLikeGraph(ts, comment, friendshipGraph));

    }


    /**
     * Registers a comment in the comment store
     *
     * @param commentID the comment id
     * @param ts        the arrival time of the comment
     */
    public void registerComment(long commentID, long ts, String comment) {

        registerComment(commentID, ts, comment, false);
    }

    /**
     * Registers a like in the comment store
     *
     * @param userID    the userID
     * @param commentID the comment id
     */
    public void registerLike(long commentID, long userID) {
        CommentLikeGraph commentLikeGraph = commentStore.get(commentID);
        if (commentLikeGraph != null) {
            commentLikeGraph.registerLike(userID);
        }

    }

    /**
     * Handles a new friendship
     *
     * @param uId1 the userID of friend one
     * @param uId2 the userID of friend two
     */
    public void handleNewFriendship(long uId1, long uId2) {
        for (CommentLikeGraph commentLikeGraph : commentStore.values()) {
            commentLikeGraph.handleNewFriendship(uId1, uId2);
        }

    }

    /**
     * Get the number of comments in the comment store
     *
     * @return the number of comments
     */
    public long getNumberOfComments() {

        return commentStore.size();
    }

}
