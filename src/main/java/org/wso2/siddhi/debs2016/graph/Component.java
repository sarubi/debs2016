package org.wso2.siddhi.debs2016.graph;

/**
 * Created by malithjayasinghe on 3/20/16.
 */
public class Component {
    private long uId;
    private long nId;

    /**
     * Sets the user id
     *
     * @param uId the UID to set
     */
    public void setUId(long uId){
        this.uId=uId;
    }

    /**
     * Sets the node ID
     **
     * @param nId node ID to set
     */
    public void setNId(long nId){
        this.nId=nId;
    }

    /**
     * Gets the user ID
     *
     *
     * @return the user id
     */
    public long getUId(){
        return uId;
    }

    /**
     * Gets the node id
     *
     * @return the node id
     */
    public long getNId(){
        return nId;
    }

    /**
     * The constructor
     *
     * @param userId the user id
     * @param nodeId the node id
     */
    public Component(long userId,long nodeId){
        setUId(userId);
        setNId(nodeId);
    }
}
