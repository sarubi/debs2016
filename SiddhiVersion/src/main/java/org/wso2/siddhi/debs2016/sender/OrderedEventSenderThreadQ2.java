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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.LinkedBlockingQueue;

public class OrderedEventSenderThreadQ2 extends Thread {

    private final LinkedBlockingQueue<Object[]>[] eventBufferList;
    private InputHandler inputHandler;
    public boolean doneFlag = false;
    int k;
    long duration;

    /**
     * The constructor
     *
     * @param eventBuffer the event buffer array
     */

    public OrderedEventSenderThreadQ2(LinkedBlockingQueue<Object[]> eventBuffer[], InputHandler inputHandler, int k, long duration) {
        super("Event Sender Query 2");
        this.eventBufferList = eventBuffer;
        this.inputHandler = inputHandler;
        this.k = k;
        this.duration = duration * 1000;
    }

    /**
     *
     * Start the data reader thread
     *
     */
    public void run(){
        Object[] friendshipEvent = null;
        Object[] commentEvent = null;
        Object[] likeEvent = null;
        long startTime;
        long systemCurrentTime;
        boolean firstEvent = true;
        int flag = Constants.NO_EVENT;
        boolean friendshipLastEventArrived = false;
        boolean commentsLastEventArrived = false;
        boolean likesLastEventArrived = false;

        while (true) {
            try {
                if (firstEvent) {
                    Object[] finalFriendshipEvent = new Object[]{
                            0L,
                            -1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            duration,
                            k,
                            0,
                    };
                    systemCurrentTime = System.currentTimeMillis();
                    finalFriendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                    inputHandler.send(systemCurrentTime, finalFriendshipEvent);
                    Date startDateTime = new Date();
                    startTime = startDateTime.getTime();
                    SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
                    System.out.println("Starting the experiment at : " + startTime + "--" + ft.format(startDateTime));
                    firstEvent = false;
                }

                try {
                    if (flag == Constants.FRIENDSHIPS) {
                        if (!friendshipLastEventArrived) {
                            friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                            long lastEvent = (Long) friendshipEvent[0];
                            if (lastEvent == -1L) {
                                friendshipEvent = null;
                                friendshipLastEventArrived = true;
                            }
                        }
                    } else if (flag == 1) {
                        if (!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L) {

                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }
                        }
                    } else if (flag == 2) {
                        if (!likesLastEventArrived) {
                            likeEvent = eventBufferList[Constants.LIKES].take();
                            long lastEvent = (Long) likeEvent[0];

                            if (lastEvent == -1L) {
                                likeEvent = null;
                                likesLastEventArrived = true;
                            }
                        }
                    } else {

                        if (!friendshipLastEventArrived) {
                            friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
                            long lastEvent = (Long) friendshipEvent[0];
                            if (lastEvent == -1L) {
                                friendshipEvent = null;
                                friendshipLastEventArrived = true;
                            }
                        }
                        if (!commentsLastEventArrived) {
                            commentEvent = eventBufferList[Constants.COMMENTS].take();
                            long lastEvent = (Long) commentEvent[0];
                            if (lastEvent == -1L) {

                                commentEvent = null;
                                commentsLastEventArrived = true;
                            }

                        }
                        if (!likesLastEventArrived) {
                            likeEvent = eventBufferList[Constants.LIKES].take();
                            long lastEvent = (Long) likeEvent[0];
                            if (lastEvent == -1L) {
                                likeEvent = null;
                                likesLastEventArrived = true;
                            }
                        }
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                long timestampFriendship;
                long timestampComment;
                long timestampLike;

                if (friendshipEvent == null) {
                    timestampFriendship = Long.MAX_VALUE;
                } else {
                    timestampFriendship = (Long) friendshipEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (commentEvent == null) {
                    timestampComment = Long.MAX_VALUE;
                } else {
                    timestampComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (likeEvent == null) {
                    timestampLike = Long.MAX_VALUE;
                } else {
                    timestampLike = (Long) likeEvent[Constants.EVENT_TIMESTAMP_FIELD];
                }

                if (timestampFriendship <= timestampComment && timestampFriendship <= timestampLike && timestampFriendship != Long.MAX_VALUE) {
                    systemCurrentTime = System.currentTimeMillis();
                    friendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                    inputHandler.send(systemCurrentTime, friendshipEvent);
                    flag = Constants.FRIENDSHIPS;
                } else if (timestampComment <= timestampFriendship && timestampComment <= timestampLike && timestampComment != Long.MAX_VALUE) {
                    systemCurrentTime = System.currentTimeMillis();
                    commentEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                    inputHandler.send(systemCurrentTime, commentEvent);
                    flag = Constants.COMMENTS;
                } else if (timestampLike != Long.MAX_VALUE) {
                    systemCurrentTime = System.currentTimeMillis();
                    likeEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime; //This corresponds to the iij_timestamp
                    inputHandler.send(systemCurrentTime, likeEvent);
                    flag = Constants.LIKES;
                }

                if (friendshipEvent == null && commentEvent == null && likeEvent == null) {
                    systemCurrentTime = System.currentTimeMillis();
                    Object[] finalFriendshipEvent = new Object[]{
                            0L,
                            -2L,
                            0L,
                            0L,
                            0L,
                            0L,
                            0L,
                            0L,
                            0,
                    };

                    finalFriendshipEvent[Constants.INPUT_INJECTION_TIMESTAMP_FIELD] = systemCurrentTime;
                    inputHandler.send(systemCurrentTime, finalFriendshipEvent);
                    doneFlag = true;
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}