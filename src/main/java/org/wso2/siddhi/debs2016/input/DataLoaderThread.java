package org.wso2.siddhi.debs2016.input;

import com.google.common.base.Splitter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
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
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            Object[] eventData;
            String user;
            while (line != null) {
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                switch(fileType) {
                    case POSTS:
                        String postsTimeStamp = dataStrIterator.next();
                        if (postsTimeStamp.equals("")){
                            break;
                        }
                        DateTime dt = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(postsTimeStamp);
                        long postsTimeStampLong = dt.getMillis();
                        long postID = Long.parseLong(dataStrIterator.next());
                        long userID = Long.parseLong(dataStrIterator.next());
                        String post = dataStrIterator.next();
                        user = dataStrIterator.next();
                        eventData = new Object[]{
                                0L,
                                postsTimeStampLong,
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
                        String commentTimeStamp = dataStrIterator.next();
                        if (commentTimeStamp.equals("")){
                            break;
                        }
                        DateTime dt2 = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(commentTimeStamp);
                        long commentTimeStampLong = dt2.getMillis();
                        long commentID = Long.parseLong(dataStrIterator.next());
                        userID = Long.parseLong(dataStrIterator.next());
                        String comment = dataStrIterator.next();
                        user = dataStrIterator.next();
                        String commentReplied = dataStrIterator.next();

                        if(commentReplied.equals("")){
                            commentReplied = MINUS_ONE;
                        }

                        long comment_replied = Long.parseLong(commentReplied);
                        String postCommented = dataStrIterator.next();

                        if(postCommented.equals("")){
                            postCommented = MINUS_ONE;
                        }
                        long post_commented = Long.parseLong(postCommented);
                        eventData = new Object[]{
                                0L,
                                commentTimeStampLong,
                                userID,
                                commentID,
                                comment,
                                user,
                                comment_replied,
                                post_commented,
                                Constants.COMMENTS
                        };
                        eventBufferList.put(eventData);

                        break;
                    case FRIENDSHIPS:
                        String friendshipsTimeStamp = dataStrIterator.next();
                        if (friendshipsTimeStamp.equals("")){
                            break;
                        }
                        DateTime dt3 = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStamp);
                        long  friendshipTimeStampLong = dt3.getMillis();
                        long user1ID = Long.parseLong(dataStrIterator.next());
                        long user2ID = Long.parseLong(dataStrIterator.next());
                        eventData = new Object[]{
                                0L,
                                friendshipTimeStampLong,
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
                        String likeTimeStamp = dataStrIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                        if (likeTimeStamp.equals("")){
                            break;
                        }
                        DateTime dt4 = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(likeTimeStamp);
                        long  likeTimeStampLong = dt4.getMillis();
                        userID = Long.parseLong(dataStrIterator.next());
                        commentID = Long.parseLong(dataStrIterator.next());
                        eventData = new Object[]{
                                0L,
                                likeTimeStampLong,
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
				line = br.readLine();
            }

            Long postsTimeStampLong = -1L;
            Long postID = -1L;
            Long userID = -1L;
            String post = "";
            user = "";
            eventData = new Object[]{
                    -1L,
                    postsTimeStampLong,
                    postID,
                    userID,
                    post,
                    user,
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
