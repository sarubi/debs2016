package org.wso2.siddhi.debs2016.input;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads the data from the file and store it in memory
 * 
 */
public class DataLoderThread extends Thread {
    private String fileName;
    private static Splitter splitter = Splitter.on('|');
    private LinkedBlockingQueue<Object[]> eventBufferList;
    private BufferedReader br;
    private int count;
    private FileType fileType;
    private String MINUS_ONE = "-1";

    /**
     * The constructor
     *
     * @param fileName the name of the file to be read
     * @param eventBuffer the blocking queue which stores the data read from the file
     * @param fileType the type of the file to be read
     */
    public DataLoderThread(String fileName, LinkedBlockingQueue<Object[]> eventBuffer, FileType fileType){
        super("Data Loader");
        this.fileName = fileName;
        this.eventBufferList = eventBuffer;
        this.fileType = fileType;
    }

    public void run() {
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            Object[] eventData = null;
            Long userID = -1l;
            String user = null;

            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();

                switch(fileType) {
                    case POSTS:
                        //ts long, post_id long, user_id long, post string, user string
                        String postsTimeStamp = dataStrIterator.next(); //e.g., 2010-02-01T05:12:32.921+0000
                        Long postID = Long.parseLong(dataStrIterator.next());
                        userID = Long.parseLong(dataStrIterator.next());
                        String post = dataStrIterator.next();
                        user = dataStrIterator.next();

                        eventData = new Object[]{
                                0l,//We need to attach the time when we are injecting an event to the query network.
                                // For that we have to set a separate field which will be populated when we are
                                // injecting an event to the input stream.
                                postsTimeStamp,
                                postID,
                                userID,
                                post,
                                user
                        };

                        eventBufferList.put(eventData);
                        break;
                    case COMMENTS:
                        //ts long, comment_id long, user_id long, comment string, user string, comment_replied long,
                        // post_commented long

                        String commentTimeStamp = dataStrIterator.next(); //e.g., 2010-02-09T04:05:20.777+0000
                        Long commentID = Long.parseLong(dataStrIterator.next());
                        userID = Long.parseLong(dataStrIterator.next());
                        String comment = dataStrIterator.next();
                        user = dataStrIterator.next();
                        String commentReplied = dataStrIterator.next();

                        if(commentReplied.equals("")){
                            commentReplied = MINUS_ONE;
                        }

                        Long comment_replied = Long.parseLong(commentReplied);
                        Long post_commented = Long.parseLong(dataStrIterator.next());

                        eventData = new Object[]{
                                0l,//We need to attach the time when we are injecting an event to the query network.
                                // For that we have to set a separate field which will be populated when we are
                                // injecting an event to the input stream.
                                commentTimeStamp,
                                commentID,
                                userID,
                                comment,
                                user,
                                comment_replied,
                                post_commented
                        };

                        eventBufferList.put(eventData);
                        break;
                    case FRIENDSHIPS:
                        break;

                    case LIKES:
                        break;
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NumberFormatException ec){
            ec.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}
