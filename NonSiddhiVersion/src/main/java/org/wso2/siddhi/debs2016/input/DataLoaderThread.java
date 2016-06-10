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

package org.wso2.siddhi.debs2016.input;

import com.google.common.base.Splitter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads the data from the file and store it in memory
 * 
 */
public class DataLoaderThread extends Thread {
    private final String fileName;
    private final static Splitter splitter = Splitter.on('|');
    private final LinkedBlockingQueue<Object[]> eventBufferList;
    private final FileType fileType;
    private static final String MINUS_ONE = "-1";

    /**
     * The constructor
     *
     * @param fileName the name of the file to be read
     * @param fileType the type of the file to be read
     * @param bufferLimit the size limit of queues
     */
    public DataLoaderThread(String fileName, FileType fileType, int bufferLimit){
        super("Data Loader");
        this.fileName = fileName;
        this.fileType = fileType;
        eventBufferList = new LinkedBlockingQueue<>(bufferLimit);
    }

    public void run() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024)){
            String line = bufferedReader.readLine();
            Object[] eventData;
            String user;
            while (line != null) {
                Iterator<String> dataStreamIterator = splitter.split(line).iterator();
                switch(fileType) {
                    case POSTS:
                        String postsTimeStampString = dataStreamIterator.next();
                        if (("").equals(postsTimeStampString)){
                            break;
                        }
                        DateTime postDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(postsTimeStampString);
                        long postsTimeStamp = postDateTime.getMillis();
                        long postID = Long.parseLong(dataStreamIterator.next());
                        long userID = Long.parseLong(dataStreamIterator.next());
                        String post = dataStreamIterator.next();
                        user = dataStreamIterator.next();
                        eventData = new Object[]{
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
                        eventBufferList.put(eventData);

                        break;
                    case COMMENTS:
                        String commentTimeStampString = dataStreamIterator.next();
                        if (("").equals(commentTimeStampString)){
                            break;
                        }
                        DateTime commentDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(commentTimeStampString);
                        long commentTimeStamp = commentDateTime.getMillis();
                        long commentID = Long.parseLong(dataStreamIterator.next());
                        userID = Long.parseLong(dataStreamIterator.next());
                        String comment = dataStreamIterator.next();
                        user = dataStreamIterator.next();
                        String commentReplied = dataStreamIterator.next();

                        if(("").equals(commentReplied)){
                            commentReplied = MINUS_ONE;
                        }

                        long commentRepliedId = Long.parseLong(commentReplied);
                        String postCommented = dataStreamIterator.next();

                        if(("").equals(postCommented)){
                            postCommented = MINUS_ONE;
                        }
                        long postCommentedId = Long.parseLong(postCommented);
                        eventData = new Object[]{
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
                        eventBufferList.put(eventData);

                        break;
                    case FRIENDSHIPS:
                        String friendshipsTimeStampString = dataStreamIterator.next();
                        if (("").equals(friendshipsTimeStampString)){
                            break;
                        }
                        DateTime friendshipDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStampString);
                        long  friendshipTimeStamp = friendshipDateTime.getMillis();
                        long user1ID = Long.parseLong(dataStreamIterator.next());
                        long user2ID = Long.parseLong(dataStreamIterator.next());
                        eventData = new Object[]{
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
                        eventBufferList.put(eventData);
                        break;
                    case LIKES:
                        String likeTimeStampString = dataStreamIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                        if (("").equals(likeTimeStampString)){
                            break;
                        }
                        DateTime likeDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(likeTimeStampString);
                        long  likeTimeStamp = likeDateTime.getMillis();
                        userID = Long.parseLong(dataStreamIterator.next());
                        commentID = Long.parseLong(dataStreamIterator.next());
                        eventData = new Object[]{
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
                        eventBufferList.put(eventData);
                        break;
                }
				line = bufferedReader.readLine();
            }

            Long postsTimeStampLong = -1L;
            Long postID = -1L;
            Long userID = -1L;
            eventData = new Object[]{
                    -1L,
                    postsTimeStampLong,
                    postID,
                    userID,
                    0L,
                    0L,
                    0L,
                    0L,
                    Constants.POSTS
            };
            eventBufferList.put(eventData);

        } catch (NumberFormatException | InterruptedException | IOException e) {
            e.printStackTrace();
        }


    }

    /**
     *
     * @return the event buffer which has the event data
     */
    public LinkedBlockingQueue<Object[]> getEventBuffer()
    {
        return eventBufferList;
    }


}
