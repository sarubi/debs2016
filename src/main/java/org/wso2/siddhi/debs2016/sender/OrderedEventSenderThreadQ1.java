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

package org.wso2.siddhi.debs2016.sender;

import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.debs2016.util.Constants;

import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

public class OrderedEventSenderThreadQ1 extends Thread {

    private LinkedBlockingQueue<Object[]> eventBufferList[];
    private InputHandler inputHandler;
    private Date startDateTime;
    public boolean doneFlag = false;

    /**
     * The constructor
     *
     * @param eventBuffer  the event buffer array
     */
    public OrderedEventSenderThreadQ1(LinkedBlockingQueue<Object[]> eventBuffer[], InputHandler inputHandler) {
        super("Event Sender Query 1");
        this.eventBufferList = eventBuffer;
        this.inputHandler = inputHandler;
    }


    public void run() {
        Object[] commentEvent = null;
        Object[] postEvent = null;
        long systemCurrentTime;
        boolean firstEvent = true;
        int flag = Constants.NO_EVENT;
        boolean postLastEventArrived = false;
        boolean commentsLastEventArrived = false;
        while (true) {

            try {
                if (firstEvent) {
                    Object[] firstPostEvent = new Object[]{
                            0L,
                            -1L,
                            0L,
                            0L,
                            "",
                            "",
                            0L,
                            0L,
                            Constants.POSTS
                    };
                    systemCurrentTime = System.currentTimeMillis();
                    firstPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                    inputHandler.send(systemCurrentTime, firstPostEvent);
                    //We print the start and the end times of the experiment even if the performance logging is disabled.
                    firstEvent = false;
                }

                try {
                    if (flag == Constants.POSTS) {
                        if(!postLastEventArrived) {
                            postEvent = eventBufferList[Constants.POSTS].take();
                            long lastEvent = (Long) postEvent[0];
                            if (lastEvent == -1L)
                            {
                                postEvent = null;
                                postLastEventArrived = true;
                            }
                        }
                    } else if (flag == Constants.COMMENTS) {
                        if(!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L)
                            {
                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }
                        }
                    } else {
                        if(!postLastEventArrived) {
                            postEvent = eventBufferList[Constants.POSTS].take();
                            long lastEvent = (Long) postEvent[0];
                            if (lastEvent == -1L)
                            {
                                postEvent = null;
                                postLastEventArrived = true;
                            }
                        }
                        if(!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L)
                            {
                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }
                        }
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                long timestampComment;
                long timestampPost;

                if (commentEvent == null) {
                    timestampComment = Long.MAX_VALUE;
                } else {
                    timestampComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (postEvent == null) {
                    timestampPost = Long.MAX_VALUE;
                } else {
                    timestampPost = (Long) postEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (timestampComment < timestampPost && timestampComment != Long.MAX_VALUE) {

                    systemCurrentTime = System.currentTimeMillis();
                    commentEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                    inputHandler.send(systemCurrentTime, commentEvent);
                    flag = Constants.COMMENTS;
                } else if (timestampPost != Long.MAX_VALUE) {

                    systemCurrentTime = System.currentTimeMillis();
                    postEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                    inputHandler.send(systemCurrentTime, postEvent);
                    flag = Constants.POSTS;
                }

                //When all buffers are empty
                if (commentEvent == null && postEvent == null) {
                    //Sending second dummy event to signal end of streams
                    Object[] finalPostEvent = new Object[]{
                            0L,
                            -2L,
                            0L,
                            0L,
                            "",
                            "",
                            0L,
                            0L,
                            Constants.POSTS
                    };
                    systemCurrentTime = System.currentTimeMillis();
                    finalPostEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                    Thread.sleep(1000);//We just sleep for short period so that we can ensure that all the data events have been processed by the ranker properly before we shutdown.
                    inputHandler.send(systemCurrentTime, finalPostEvent);
                    doneFlag = true;
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}