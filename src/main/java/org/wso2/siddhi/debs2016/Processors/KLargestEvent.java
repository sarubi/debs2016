package org.wso2.siddhi.debs2016.Processors;

/**
 * Created by malithjayasinghe on 4/9/16.
 */
public class KLargestEvent {

    private String [] kLargestComment;
    private long timeStamp;

    /**
     *
     * The constructor
     *
     * @param kLargestComment k largest comment array
     * @param timeStamp time stamp
     */
    public KLargestEvent(String [] kLargestComment, long timeStamp){

        this.kLargestComment = kLargestComment;
        this.timeStamp = timeStamp;
    }

    /**
     *
     * Gets the k largest connected array
     *
     * @return the k largest connected array
     */
    public String[] getkLargestComment()
    {
        return kLargestComment;
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

}
