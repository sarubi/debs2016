package org.wso2.siddhi.debs2016.input;

import com.google.common.base.Splitter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

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
    private String fileName;
    private final static Splitter splitter = Splitter.on('|');
    private LinkedBlockingQueue<Object[]> eventBufferList;
    private BufferedReader br;
    private int count;
    private FileType fileType;
    private String MINUS_ONE = "-1";
    private boolean debug = true;

    /**
     * The constructor
     *
     * @param fileName the name of the file to be read
     * @param eventBuffer the blocking queue which stores the data read from the file
     * @param fileType the type of the file to be read
     */
    public DataLoaderThread(String fileName, LinkedBlockingQueue<Object[]> eventBuffer, FileType fileType){
        super("Data Loader");
        this.fileName = fileName;
        this.eventBufferList = eventBuffer;
        this.fileType = fileType;
    }

    public void run() {
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            Object[] eventData;
            String user ;

            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                switch(fileType) {
                    case POSTS:
                        //ts long, post_id long, user_id long, post string, user string
                        String postsTimeStamp = dataStrIterator.next(); //e.g., 2010-02-01T05:12:32.921+0000
//                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
//                        String startDate = "2013-07-12T18:31:01.000Z";
                        DateTime dt = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(postsTimeStamp);
                        Long postsTimeStampLong = dt.getMillis();
                        Long postID = Long.parseLong(dataStrIterator.next());
                        Long userID = Long.parseLong(dataStrIterator.next());
                        String post = dataStrIterator.next();
                        user = dataStrIterator.next();
                        eventData = new Object[]{
                                0l,//We need to attach the time when we are injecting an event to the query network.
                                // For that we have to set a separate field which will be populated when we are
                                // injecting an event to the input stream.
                                postsTimeStampLong,
                                postID,
                                userID,
                                post,
                                user
                        };
                        //System.out.println(count++);
                        eventBufferList.put(eventData);
                        break;
                    case COMMENTS:
                        //ts long, comment_id long, user_id long, comment string, user string, comment_replied long,
                        // post_commented long
                        String commentTimeStamp = dataStrIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                        DateTime dt2 = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(commentTimeStamp);
                        Long commentTimeStampLong = dt2.getMillis();
                        Long commentID = Long.parseLong(dataStrIterator.next());
                        userID = Long.parseLong(dataStrIterator.next());
                        String comment = dataStrIterator.next();
                        user = dataStrIterator.next();
                        String commentReplied = dataStrIterator.next();

                        if(commentReplied.equals("")){
                            commentReplied = MINUS_ONE;
                        }

                        Long comment_replied = Long.parseLong(commentReplied);
                        String postCommented = dataStrIterator.next();

                        if(postCommented.equals("")){
                            postCommented = MINUS_ONE;
                        }

                        Long post_commented = Long.parseLong(postCommented);

                        eventData = new Object[]{
                                0l,//We need to attach the time when we are injecting an event to the query network.
                                // For that we have to set a separate field which will be populated when we are
                                // injecting an event to the input stream.
                                commentTimeStampLong,
                                userID,
                                commentID,
                                comment,
                                user,
                                comment_replied,
                                post_commented
                        };
                        eventBufferList.put(eventData);
                        break;
                    case FRIENDSHIPS:
                        String friendshipsTimeStamp = dataStrIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                        DateTime dt3 = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStamp);
                        Long  friendshipTimeStampLong = dt3.getMillis();
                        Long user1ID = Long.parseLong(dataStrIterator.next());
                        Long user2ID = Long.parseLong(dataStrIterator.next());
                        if(debug == true) {
                            System.out.println("data loader even buffer size " + eventBufferList.size() + ", ts = " + friendshipTimeStampLong + ", user_1_ID = " + user1ID + ", user_2_ID = " + user2ID + "\n");
                        }
                        eventData = new Object[]{
                                0l,//We need to attach the time when we are injecting an event to the query network.
                                // For that we have to set a separate field which will be populated when we are
                                // injecting an event to the input stream.
                                friendshipTimeStampLong,
                                user1ID,
                                user2ID,
                        };
                        eventBufferList.put(eventData);
                        break;
                    case LIKES:
                        String likeTimeStamp = dataStrIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                        DateTime dt4 = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(likeTimeStamp);
                        Long  likeTimeStampLong = dt4.getMillis();
                        userID = Long.parseLong(dataStrIterator.next());
                        commentID = Long.parseLong(dataStrIterator.next());
                        if(debug == true) {
                            System.out.println("data loader even buffer size " + eventBufferList.size() + ", ts = " + likeTimeStampLong + ", user_id = " + userID + ", comment_ID = " + commentID + "\n");
                        }
                        eventData = new Object[]{
                                0l,//We need to attach the time when we are injecting an event to the query network.
                                // For that we have to set a separate field which will be populated when we are
                                // injecting an event to the input stream.
                                likeTimeStampLong,
                                userID,
                                commentID,
                        };
                        eventBufferList.put(eventData);

                        break;
                }
				line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException ec){
            ec.printStackTrace();
        } catch (IOException ec){
            ec.printStackTrace();
        } catch (InterruptedException e){
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
