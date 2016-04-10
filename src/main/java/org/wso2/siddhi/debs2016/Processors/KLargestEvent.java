package org.wso2.siddhi.debs2016.Processors;

import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by malithjayasinghe on 4/9/16.
 */
public class KLargestEvent {

    private Multimap<Long, String> kLargestComment;
    private long timeStamp;
    private int handlerID;


    public void setKLargestComment(Multimap<Long, String> kLargestComment)
    {
        this.kLargestComment = kLargestComment;
    }

    public void setTimeStamp(long timestamp)
    {
        this.timeStamp = timestamp;
    }

    /**
     *
     * Gets the k largest connected array
     *
     * @return the k largest connected array
     */
    public Multimap<Long, String> getkLargestComment()
    {
        return kLargestComment;
    }

    public String toString()
    {

        StringBuilder builder = new StringBuilder();
        builder.append("handlerID " + handlerID + " ");

        for (String comment:
             kLargestComment.values()) {
            builder.append(comment + ",");
        }

        builder.append("\n");
        return builder.toString();
    }

    /**
     * The time stamp of the event
     *
     * @return the event time
     */
    public long getTimeStamp()
    {
        return timeStamp;
    }

    public void setEventHandler(int handlerID)
    {
        this.handlerID = handlerID;
    }

    public int getHandlerID()
    {
        return handlerID;
    }

}
