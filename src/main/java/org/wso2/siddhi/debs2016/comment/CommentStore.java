package org.wso2.siddhi.debs2016.comment;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import org.wso2.siddhi.debs2016.graph.CommentLikeGraph;
import org.wso2.siddhi.debs2016.graph.Graph;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class CommentStore {

    private final long duration;
    private final HashMap<Long, CommentLikeGraph> commentStore = new HashMap<>(); //Comment ID, CLG
    private String[] previousKComments;
    private long timestampTriggeredChange;
    private final Graph friendshipGraph;
    private String[] kComments;
    private final int k ;
    private final StringBuilder builder = new StringBuilder();
    private BufferedWriter writer;
    private final Multimap<Long, String> componentSizeCommentMap = TreeMultimap.create(Comparator.<Long>reverseOrder(), Comparator.<String>naturalOrder()); //sizeOfComponent, comment
    private final LinkedList<CommentComponent> commentTimeWindow = new LinkedList<>();

    /**
     * The constructor
     *
     * @param duration the duration
     */
    public CommentStore(long duration, Graph friendshipGraph, int k) {
        this.duration = duration;
        this.friendshipGraph = friendshipGraph;
        this.k = k;
        kComments = new String[k];
        previousKComments = new String[k];
        File q2 = new File("q2.txt");
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
        timestampTriggeredChange = time;
        for(Iterator<CommentComponent> iterator = commentTimeWindow.iterator(); iterator.hasNext();){
            CommentComponent commentComponent=iterator.next();
            long arrivalTime = commentComponent.getTimestamp();
            long lifeTime = time - arrivalTime;
            long commentId = commentComponent.getCommentId();
            if(duration  < lifeTime){
                iterator.remove();
                CommentLikeGraph commentLikeGraph = commentStore.get(commentId);
                long size = commentLikeGraph.getSizeOfLargestConnectedComponent();
                String comment = commentLikeGraph.getComment();
                componentSizeCommentMap.remove(size, comment);
                commentStore.remove(commentId);
            }
            else {
                break;
            }
        }

    }

    /**
     * print the k largest comments if there is change in the order
     *
     * @param delimiter the delimiter to be printed in between outputs
     * @param printKComments true would print in terminal. False will not print in terminal
     * @param writeToFile true would write to file. false will not write to file
     */
    public long computeKLargestComments(String delimiter, boolean printKComments, boolean writeToFile) {

        try {
            if (hasKLargestCommentsChanged()) {
                builder.setLength(0);
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                String formattedDate = dateFormat.format(new java.util.Date(timestampTriggeredChange));
                builder.append(formattedDate);
                for (String print : previousKComments) {
                    builder.append(delimiter).append(print);
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
     */
    public void destroy(){
        try {
            writer.close();
        }catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    /**
     * Check if the k largest comments have changed
     *
     * @return true if if it has changed false otherwise
     */
    private boolean hasKLargestCommentsChanged()
    {
        boolean changeFlag = false;
        kComments = new String[k];
        if (componentSizeCommentMap != null) {
            int limit = (k <= componentSizeCommentMap.size() ? k : componentSizeCommentMap.size());
            int i = 0;

            for (String comment: componentSizeCommentMap.values()){
                kComments[i] = comment;
                if (previousKComments == null || !(kComments[i].equals(previousKComments[i]))) {
                    changeFlag = true;
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
            if (changeFlag) {
                previousKComments = kComments;
            }
        }
        return changeFlag;
    }

    /**
     * Registers a comment in the comment store
     *
     * @param commentId the comment id
     * @param timestamp the arrival time of the comment
     * @param comment the comment string
     */
    public void registerComment(long commentId, long timestamp, String comment) {

        commentStore.put(commentId, new CommentLikeGraph(comment, friendshipGraph));
        commentTimeWindow.add(new CommentComponent(timestamp, commentId));
    }


    /**
     * Registers a like in the comment store
     *
     * @param userId    the userID
     * @param commentId the comment id
     */
    public void registerLike(long commentId, long userId) {
        CommentLikeGraph commentLikeGraph = commentStore.get(commentId);
        if (commentLikeGraph != null) {
            commentLikeGraph.registerLike(userId, componentSizeCommentMap);
        }

    }

    /**
     * Handles a new friendship
     *
     * @param userOneId the userID of friend one
     * @param userTwoId the userID of friend two
     */
    public void handleNewFriendship(long userOneId, long userTwoId) {
        for (CommentLikeGraph commentLikeGraph : commentStore.values()) {
            commentLikeGraph.handleNewFriendship(userOneId, userTwoId, componentSizeCommentMap);
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
