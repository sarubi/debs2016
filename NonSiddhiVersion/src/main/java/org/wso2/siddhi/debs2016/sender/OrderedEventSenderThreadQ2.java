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
import org.wso2.siddhi.debs2016.Processors.Q2EventManager;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

public class OrderedEventSenderThreadQ2{

    private static BufferedReader friendshipsReader;
    private static BufferedReader commentsReader;
    private static BufferedReader likesReader;


    private static final int BUFFERING_IN_READERS = 1024 * 1024 * 16;
    private static Object[] debsEvents = new Object[3];
    private final static Splitter splitter = Splitter.on('|');
    private static final String MINUS_ONE = "-1";

    /**
     * The constructor
     *
     */
    public OrderedEventSenderThreadQ2(String friendshipFile, String commentsFile, String likesFile) {

        try {
            friendshipsReader = new BufferedReader(new InputStreamReader(new FileInputStream(friendshipFile), "UTF-8"), OrderedEventSenderThreadQ2.BUFFERING_IN_READERS);
            commentsReader = new BufferedReader(new InputStreamReader(new FileInputStream(commentsFile), "UTF-8"), OrderedEventSenderThreadQ2.BUFFERING_IN_READERS);
            likesReader = new BufferedReader(new InputStreamReader(new FileInputStream(likesFile), "UTF-8"), OrderedEventSenderThreadQ2.BUFFERING_IN_READERS);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            String friendshipLine = friendshipsReader.readLine();
            Iterator<String> friendshipStreamIterator = splitter.split(friendshipLine).iterator();

            String friendshipsTimeStampString = friendshipStreamIterator.next();
            if (("").equals(friendshipsTimeStampString)) {
                //TODO
            } else {
                DateTime friendshipDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStampString);
                long  friendshipTimeStamp = friendshipDateTime.getMillis();
                long user1ID = Long.parseLong(friendshipStreamIterator.next());
                long user2ID = Long.parseLong(friendshipStreamIterator.next());
                Object[] friendshipData = new Object[]{
                        0L,
                        friendshipTimeStamp,
                        user1ID,
                        user2ID,
                        0L,
                        0L,
                        0L,
                        0L,
                        Constants.FRIENDSHIPS
                };
                debsEvents[0] = friendshipData;
            }


            String commentLine = commentsReader.readLine();
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

            String likeLine = likesReader.readLine();
            Iterator<String> likeStreamIterator = splitter.split(likeLine).iterator();

            String likeTimeStampString = likeStreamIterator.next();
            if (("").equals(likeTimeStampString)) {
                //TODO
            } else {
                DateTime likeDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(likeTimeStampString);
                long  likeTimeStamp = likeDateTime.getMillis();
                long userID = Long.parseLong(likeStreamIterator.next());
                long commentID = Long.parseLong(likeStreamIterator.next());
                Object[] likeData  = new Object[]{
                        0L,
                        likeTimeStamp,
                        userID,
                        commentID,
                        0L,
                        0L,
                        0L,
                        0L,
                        Constants.LIKES
                };
                debsEvents[2] = likeData;

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

        for (byte i = 0; i < 3; i++) {
            Object[] singleEvent = (Object[]) debsEvents[i];

            if ((long) singleEvent[1] < lowestTimestamp) {
                indexOfNextEvent = i;
                lowestTimestamp = (long) singleEvent[1];
            }
        }

        try {
            Object[] topPriority = (Object[]) debsEvents[indexOfNextEvent];
            switch ((int) topPriority[8]) {
                case Constants.FRIENDSHIPS: //Friendship
                    nextEvent = (Object[]) debsEvents[0];

                    in = friendshipsReader.readLine();

                    if (in != null && !in.isEmpty()){
                        Iterator<String> friendshipStreamIterator = splitter.split(in).iterator();

                        String friendshipsTimeStampString = friendshipStreamIterator.next();
                        if (("").equals(friendshipsTimeStampString)) {
                            //TODO
                        } else {
                            DateTime friendshipDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStampString);
                            long  friendshipTimeStamp = friendshipDateTime.getMillis();
                            long user1ID = Long.parseLong(friendshipStreamIterator.next());
                            long user2ID = Long.parseLong(friendshipStreamIterator.next());
                            Object[] friendshipData = new Object[]{
                                    0L,
                                    friendshipTimeStamp,
                                    user1ID,
                                    user2ID,
                                    0L,
                                    0L,
                                    0L,
                                    0L,
                                    Constants.FRIENDSHIPS
                            };
                            debsEvents[0] = friendshipData;
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

                    in = commentsReader.readLine();

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
                case Constants.LIKES: //Likes

                    nextEvent = (Object[]) debsEvents[2];

                    in = likesReader.readLine();

                    if (in != null && !in.isEmpty()) {

                        Iterator<String> likeStreamIterator = splitter.split(in).iterator();

                        String likeTimeStampString = likeStreamIterator.next();
                        if (("").equals(likeTimeStampString)) {
                            //TODO
                        } else {
                            DateTime likeDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(likeTimeStampString);
                            long  likeTimeStamp = likeDateTime.getMillis();
                            long userID = Long.parseLong(likeStreamIterator.next());
                            long commentID = Long.parseLong(likeStreamIterator.next());
                            Object[] likeData  = new Object[]{
                                    0L,
                                    likeTimeStamp,
                                    userID,
                                    commentID,
                                    0L,
                                    0L,
                                    0L,
                                    0L,
                                    Constants.LIKES
                            };
                            debsEvents[2] = likeData;
                        }

                        return nextEvent;
                    } else {
                        debsEvents[2] = new Object[]{
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
                            0L,
                            0L,
                            0L,
                            0L,
                            0,
                    };
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }










    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


//    /**
//     *
//     * Start the data reader thread
//     *
//     */
//    public void run(){
//        Object[] friendshipEvent = null;
//        Object[] commentEvent = null;
//        Object[] likeEvent = null;
//        long startTime;
//        long systemCurrentTime;
//        boolean firstEvent = true;
//        int flag = Constants.NO_EVENT;
//        boolean friendshipLastEventArrived = false;
//        boolean commentsLastEventArrived = false;
//        boolean likesLastEventArrived = false;
//
//        while(true){
//            if(firstEvent){
//                Object[] finalFriendshipEvent = new Object[]{
//                        0L,
//                        -1L,
//                        0L,
//                        0L,
//                        0L,
//                        0L,
//                        0L,
//                        0L,
//                        0,
//                };
//                systemCurrentTime = System.currentTimeMillis();
//                DEBSEvent event = manager.getNextDebsEvent();
//                event.setObjectArray(finalFriendshipEvent);
//                event.setSystemArrivalTime(systemCurrentTime);
//                manager.publish();
//                Date startDateTime = new Date();
//                startTime = startDateTime.getTime();
//                SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
//                System.out.println("Starting the experiment at : " + startTime + "--" + ft.format(startDateTime));
//                firstEvent = false;
//            }
//
//            try{
//                if (flag == Constants.FRIENDSHIPS) {
//                    if(!friendshipLastEventArrived)
//                    {
////                        friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
//                        long lastEvent = (Long) friendshipEvent[0];
//                        if (lastEvent == -1L)
//                        {
//                            friendshipEvent = null;
//                            friendshipLastEventArrived = true;
//                        }
//                    }
//                }else if (flag == 1){
//                    if(!commentsLastEventArrived) {
////                        commentEvent = eventBufferList[Constants.COMMENTS].take();
//                        long lastEvent = (Long) commentEvent[0];
//                        if (lastEvent == -1L) {
//
//                            commentEvent = null;
//                            commentsLastEventArrived = true;
//                        }
//                    }
//                }else if (flag == 2) {
//                    if (!likesLastEventArrived) {
////                        likeEvent = eventBufferList[Constants.LIKES].take();
//                        long lastEvent = (Long) likeEvent[0];
//
//                        if (lastEvent == -1L) {
//                            likeEvent = null;
//                            likesLastEventArrived = true;
//                         }
//                    }
//                }else{
//
//                    if(!friendshipLastEventArrived) {
////                        friendshipEvent = eventBufferList[Constants.FRIENDSHIPS].take();
//                        long lastEvent = (Long) friendshipEvent[0];
//                        if (lastEvent == -1L)
//                        {
//                            friendshipEvent = null;
//                            friendshipLastEventArrived = true;
//                        }
//                    }
//                    if(!commentsLastEventArrived) {
////                        commentEvent = eventBufferList[Constants.COMMENTS].take();
//                        long lastEvent = (Long) commentEvent[0];
//                        if (lastEvent == -1L) {
//
//                            commentEvent = null;
//                            commentsLastEventArrived = true;
//                        }
//
//                    }
//                    if(!likesLastEventArrived) {
////                        likeEvent = eventBufferList[Constants.LIKES].take();
//                        long lastEvent = (Long) likeEvent[0];
//                        if (lastEvent == -1L) {
//                            likeEvent = null;
//                            likesLastEventArrived = true;
//                        }
//                    }
//                }
//
//            }catch (Exception ex){
//                ex.printStackTrace();
//            }
//
//            long timestampFriendship;
//            long timestampComment;
//            long timestampLike;
//
//            if (friendshipEvent == null){
//                timestampFriendship = Long.MAX_VALUE;
//            }else{
//                timestampFriendship = (Long) friendshipEvent[Constants.EVENT_TIMESTAMP_FIELD];
//            }
//
//            if (commentEvent == null){
//                timestampComment = Long.MAX_VALUE;
//            }else{
//                timestampComment = (Long) commentEvent[Constants.EVENT_TIMESTAMP_FIELD];
//            }
//
//            if (likeEvent == null){
//                timestampLike = Long.MAX_VALUE;
//            }else{
//                timestampLike = (Long) likeEvent[Constants.EVENT_TIMESTAMP_FIELD];
//            }
//
//            if (timestampFriendship <= timestampComment && timestampFriendship <= timestampLike && timestampFriendship != Long.MAX_VALUE){
//                systemCurrentTime = System.currentTimeMillis();
//                DEBSEvent debsEvent = manager.getNextDebsEvent();
//                debsEvent.setObjectArray(friendshipEvent);
//                debsEvent.setSystemArrivalTime(systemCurrentTime);
//                manager.publish();
//                flag = Constants.FRIENDSHIPS;
//            }else if (timestampComment <= timestampFriendship && timestampComment <= timestampLike && timestampComment != Long.MAX_VALUE){
//                systemCurrentTime = System.currentTimeMillis();
//                DEBSEvent debsEvent = manager.getNextDebsEvent();
//                debsEvent.setObjectArray(commentEvent);
//                debsEvent.setSystemArrivalTime(systemCurrentTime);
//                manager.publish();
//                flag = Constants.COMMENTS;
//            }else if (timestampLike != Long.MAX_VALUE){
//                systemCurrentTime = System.currentTimeMillis();
//                DEBSEvent debsEvent = manager.getNextDebsEvent();
//                debsEvent.setObjectArray(likeEvent);
//                debsEvent.setSystemArrivalTime(systemCurrentTime);
//                manager.publish();
//                flag = Constants.LIKES;
//            }
//
//            if (friendshipEvent == null && commentEvent == null && likeEvent == null){
//                systemCurrentTime = System.currentTimeMillis();
//                Object[] finalFriendshipEvent = new Object[]{
//                        0L,
//                        -2L,
//                        0L,
//                        0L,
//                        0L,
//                        0L,
//                        0L,
//                        0L,
//                        0,
//                };
//
//                DEBSEvent debsEvent = manager.getNextDebsEvent();
//                debsEvent.setObjectArray(finalFriendshipEvent);
//                debsEvent.setSystemArrivalTime(systemCurrentTime);
//                manager.publish();
//                manager.getDataReadDisruptor().shutdown();
//                break;
//            }
//        }
//
//    }
}