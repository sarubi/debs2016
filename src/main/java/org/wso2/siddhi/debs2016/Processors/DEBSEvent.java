package org.wso2.siddhi.debs2016.Processors;

/**
 * Created by bhagya on 3/30/16.
 */
public class DEBSEvent {
    private Long iij_timestamp;
    private Object[] objectArray;

    public DEBSEvent(){

    }

    /**
     * Sets the system time at the time of publishing the event
     *
     * @param iij_timestamp
     */
    public void setSystemArrivalTime(Long iij_timestamp) {
        this.iij_timestamp = iij_timestamp;
    }

    /**
     * The object array containing the stream data
     *
     * @param objectArray the stream data object array
     */
    public void setObjectArray(Object[] objectArray) {
        this.objectArray = objectArray;
    }

    /**
     * Gets the system time at the time of publishing event
     *
     * @return the system time
     */
    public Long getSystemArrivalTime() {
        return iij_timestamp;
    }

    /**
     * Gets the object array which stores the stream data
     *
     * @return the object array
     */
    public Object[] getObjectArray() {
        return objectArray;
    }
}
