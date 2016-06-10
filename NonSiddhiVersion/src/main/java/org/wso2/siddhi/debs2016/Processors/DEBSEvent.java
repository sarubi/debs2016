/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
