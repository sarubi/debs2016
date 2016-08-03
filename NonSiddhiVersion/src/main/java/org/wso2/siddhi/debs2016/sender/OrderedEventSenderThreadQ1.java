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

import com.google.common.base.Splitter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.debs2016.Processors.DEBSEvent;
import org.wso2.siddhi.debs2016.Processors.Q1EventManager;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

public class OrderedEventSenderThreadQ1 {

    private static final int BUFFERING_IN_READERS = 1024 * 1024 * 16;
    private static BufferedReader postReader;
    private static BufferedReader commentReader;
    private final static Splitter splitter = Splitter.on('|');
    private static final String MINUS_ONE = "-1";

    //Index 0 is Post. Index 1 is Comment
    private static Object[] debsEvents = new Object[2];

    /**
     * The constructor
     *
     * @param postsFile    paths to post file
     * @param commentsFile paths to comment file
     */
    public OrderedEventSenderThreadQ1(String postsFile, String commentsFile) {
        try {
            postReader = new BufferedReader(new InputStreamReader(new FileInputStream(postsFile), "UTF-8"), OrderedEventSenderThreadQ1.BUFFERING_IN_READERS);
            commentReader = new BufferedReader(new InputStreamReader(new FileInputStream(commentsFile), "UTF-8"), OrderedEventSenderThreadQ1.BUFFERING_IN_READERS);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            String postLine = postReader.readLine();
            Iterator<String> postStreamIterator = splitter.split(postLine).iterator();

            String postsTimeStampString = postStreamIterator.next();
            if (("").equals(postsTimeStampString)) {
                //TODO
            } else {
                DateTime postDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(postsTimeStampString);
                long postsTimeStamp = postDateTime.getMillis();
                long postID = Long.parseLong(postStreamIterator.next());
                long userID = Long.parseLong(postStreamIterator.next());
                String post = postStreamIterator.next();
                String user = postStreamIterator.next();
                Object[] postData = new Object[]{
                        0L,
                        postsTimeStamp,
                        postID,
                        userID,
                        post,
                        user,
                        0L,
                        0L,
                        Constants.POSTS
                };
                debsEvents[0] = postData;
            }


            String commentLine = commentReader.readLine();
            Iterator<String> commentStreamIterator = splitter.split(commentLine).iterator();

            String commentTimeStampString = commentStreamIterator.next();
            if (("").equals(commentTimeStampString)) {
                //TODO
            } else {
                DateTime commentDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(commentTimeStampString);
                long commentTimeStamp = commentDateTime.getMillis();
                long commentID = Long.parseLong(commentStreamIterator.next());
                long userID = Long.parseLong(commentStreamIterator.next());
                String comment = commentStreamIterator.next();
                String user = commentStreamIterator.next();
                String commentReplied = commentStreamIterator.next();

                if (("").equals(commentReplied)) {
                    commentReplied = MINUS_ONE;
                }

                long commentRepliedId = Long.parseLong(commentReplied);
                String postCommented = commentStreamIterator.next();

                if (("").equals(postCommented)) {
                    postCommented = MINUS_ONE;
                }
                long postCommentedId = Long.parseLong(postCommented);
                Object[] commentData = new Object[]{
                        0L,
                        commentTimeStamp,
                        userID,
                        commentID,
                        comment,
                        user,
                        commentRepliedId,
                        postCommentedId,
                        Constants.COMMENTS
                };
                debsEvents[1] = commentData;

            }


        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Object[] getNextEvent() {

        long lowestTimestamp = Long.MAX_VALUE;
        byte indexOfNextEvent = 0;
        Object[] nextEvent;
        String in = "";

        for (byte i = 0; i < 2; i++) {
            Object[] singleEvent = (Object[]) debsEvents[i];

            if ((long) singleEvent[1] < lowestTimestamp) {
                indexOfNextEvent = i;
                lowestTimestamp = (long) singleEvent[1];
            }
        }

        try {
            Object[] topPriority = (Object[]) debsEvents[indexOfNextEvent];
            switch ((int) topPriority[8]) {
                case Constants.POSTS: //Post
                    nextEvent = (Object[]) debsEvents[0];

                    in = postReader.readLine();

                    if (in != null && !in.isEmpty()){
                        Iterator<String> postStreamIterator = splitter.split(in).iterator();

                        String postsTimeStampString = postStreamIterator.next();
                        if (("").equals(postsTimeStampString)) {
                            //TODO
                            System.out.println("Error");
                        } else {
                            DateTime postDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(postsTimeStampString);
                            long postsTimeStamp = postDateTime.getMillis();
                            long postID = Long.parseLong(postStreamIterator.next());
                            long userID = Long.parseLong(postStreamIterator.next());
                            String post = postStreamIterator.next();
                            String user = postStreamIterator.next();
                            Object[] postData = new Object[]{
                                    0L,
                                    postsTimeStamp,
                                    postID,
                                    userID,
                                    post,
                                    user,
                                    0L,
                                    0L,
                                    Constants.POSTS
                            };
                            debsEvents[0] = postData;
                        }

                        return nextEvent;
                    } else{
                        debsEvents[0] = new Object[]{
                                0L,
                                Long.MAX_VALUE,
                                0L,
                                0L,
                                "",
                                "",
                                0L,
                                0L,
                                -1
                        };
                    }
                    return nextEvent;
                case Constants.COMMENTS: //Comment

                    nextEvent = (Object[]) debsEvents[1];

                    in = commentReader.readLine();

                    if (in != null && !in.isEmpty()) {
                        Iterator<String> commentStreamIterator = splitter.split(in).iterator();

                        String commentTimeStampString = commentStreamIterator.next();
                        if (("").equals(commentTimeStampString)) {
                            //TODO
                            System.out.println("Error");
                        } else {
                            DateTime commentDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(commentTimeStampString);
                            long commentTimeStamp = commentDateTime.getMillis();
                            long commentID = Long.parseLong(commentStreamIterator.next());
                            long userID = Long.parseLong(commentStreamIterator.next());
                            String comment = commentStreamIterator.next();
                            String user = commentStreamIterator.next();
                            String commentReplied = commentStreamIterator.next();

                            if (("").equals(commentReplied)) {
                                commentReplied = MINUS_ONE;
                            }

                            long commentRepliedId = Long.parseLong(commentReplied);
                            String postCommented = commentStreamIterator.next();

                            if (("").equals(postCommented)) {
                                postCommented = MINUS_ONE;
                            }
                            long postCommentedId = Long.parseLong(postCommented);
                            Object[] commentData = new Object[]{
                                    0L,
                                    commentTimeStamp,
                                    userID,
                                    commentID,
                                    comment,
                                    user,
                                    commentRepliedId,
                                    postCommentedId,
                                    Constants.COMMENTS
                            };
                            debsEvents[1] = commentData;
                        }

                        return nextEvent;
                    } else {
                        debsEvents[1] =new Object[]{
                                0L,
                                -2L,
                                0L,
                                0L,
                                "",
                                "",
                                0L,
                                0L,
                                -1
                        };
                    }
                    return nextEvent;
                case -1:
                    return new Object[]{
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
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

//    public void run() {
//        Object[] commentEvent = null;
//        Object[] postEvent = null;
//        long systemCurrentTime;
//        boolean firstEvent = true;
//        int flag = Constants.NO_EVENT;
//        boolean postLastEventArrived = false;
//        boolean commentsLastEventArrived = false;
//
//
////        String postLine = postReader.readLine();
////        String commentLine = commentReader.readLine();
//
//
//        while (true) {
//
//            try {
//                if (firstEvent) {
//                    Object[] firstPostEvent = new Object[]{
//                            0L,
//                            -1L,
//                            0L,
//                            0L,
//                            "",
//                            "",
//                            0L,
//                            0L,
//                            Constants.POSTS
//                    };
//                    systemCurrentTime = System.currentTimeMillis();
//                    DEBSEvent event = manager.getNextDebsEvent();
//                    event.setObjectArray(firstPostEvent);
//                    event.setSystemArrivalTime(systemCurrentTime);
//                    manager.publish();
//                    //We print the start and the end times of the experiment even if the performance logging is disabled.
//                    firstEvent = false;
//                }
//
//                try {
//                    if (flag == Constants.POSTS) {
//                        if (!postLastEventArrived) {
////                            postEvent = eventBufferList[Constants.POSTS].take();
//                            long lastEvent = (Long) postEvent[0];
//                            if (lastEvent == -1L) {
//                                postEvent = null;
//                                postLastEventArrived = true;
//                            }
//                        }
//                    } else if (flag == Constants.COMMENTS) {
//                        if (!commentsLastEventArrived) {
////                            commentEvent = eventBufferList[Constants.COMMENTS].take();
//                            long lastEvent = (Long) commentEvent[0];
//                            if (lastEvent == -1L) {
//                                commentEvent = null;
//                                commentsLastEventArrived = true;
//                            }
//                        }
//                    } else {
//                        if (!postLastEventArrived) {
////                            postEvent = eventBufferList[Constants.POSTS].take();
//                            long lastEvent = (Long) postEvent[0];
//                            if (lastEvent == -1L) {
//                                postEvent = null;
//                                postLastEventArrived = true;
//                            }
//                        }
//                        if (!commentsLastEventArrived) {
////                            commentEvent = eventBufferList[Constants.COMMENTS].take();
//                            long lastEvent = (Long) commentEvent[0];
//                            if (lastEvent == -1L) {
//                                commentEvent = null;
//                                commentsLastEventArrived = true;
//                            }
//                        }
//                    }
//
//                } catch (Exception ex) {
//                    ex.printStackTrace();
//                }
//
//                long timestampComment;
//                long timestampPost;
//
//                if (commentEvent == null) {
//                    timestampComment = Long.MAX_VALUE;
//                } else {
//                    timestampComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
//                }
//
//                if (postEvent == null) {
//                    timestampPost = Long.MAX_VALUE;
//                } else {
//                    timestampPost = (Long) postEvent[Constants.EVENT_TIMESTAMP_FIELD];
//                }
//
//                if (timestampComment < timestampPost && timestampComment != Long.MAX_VALUE) {
//
//                    systemCurrentTime = System.currentTimeMillis();
//                    DEBSEvent event = manager.getNextDebsEvent();
//                    event.setObjectArray(commentEvent);
//                    event.setSystemArrivalTime(systemCurrentTime);
//                    manager.publish();
//                    flag = Constants.COMMENTS;
//                } else if (timestampPost != Long.MAX_VALUE) {
//
//                    systemCurrentTime = System.currentTimeMillis();
//                    DEBSEvent event = manager.getNextDebsEvent();
//                    event.setObjectArray(postEvent);
//                    event.setSystemArrivalTime(systemCurrentTime);
//                    manager.publish();
//                    flag = Constants.POSTS;
//                }
//
//                //When all buffers are empty
//                if (commentEvent == null && postEvent == null) {
//                    //Sending second dummy event to signal end of streams
//                    Object[] finalPostEvent = new Object[]{
//                            0L,
//                            -2L,
//                            0L,
//                            0L,
//                            "",
//                            "",
//                            0L,
//                            0L,
//                            Constants.POSTS
//                    };
//                    systemCurrentTime = System.currentTimeMillis();
//                    DEBSEvent event = manager.getNextDebsEvent();
//                    event.setObjectArray(finalPostEvent);
//                    event.setSystemArrivalTime(systemCurrentTime);
//                    manager.publish();
//                    break;
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//    }
}