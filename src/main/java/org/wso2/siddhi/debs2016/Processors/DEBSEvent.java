package org.wso2.siddhi.debs2016.Processors;

/**
 * Created by bhagya on 3/30/16.
 */
public class DEBSEvent {

    Long iij_timestamp;
    private Object[] objectArray;
    public DEBSEvent(){

    }

    public void setIij_timestamp(Long iij_timestamp) {
        this.iij_timestamp = iij_timestamp;
    }

    public void setObjectArray(Object[] objectArray) {
        this.objectArray = objectArray;
    }

    public Long getIij_timestamp() {
        return iij_timestamp;
    }

    public Object[] getObjectArray() {
        return objectArray;
    }
}
