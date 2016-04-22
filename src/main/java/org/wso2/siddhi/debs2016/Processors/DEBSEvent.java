package org.wso2.siddhi.debs2016.Processors;

public class DEBSEvent {
    private Long inputTimestamp;
    private Object[] objectArray;

    public DEBSEvent(){

    }

    /**
     * Sets the system time at the time of publishing the event
     *
     * @param inputTimestamp is the system time
     */
    public void setSystemArrivalTime(Long inputTimestamp) {
        this.inputTimestamp = inputTimestamp;
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
        return inputTimestamp;
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
