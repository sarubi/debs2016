package org.wso2.siddhi.debs2016.sender;

import com.google.common.base.Splitter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.debs2016.Processors.Q1EventSingle;
import org.wso2.siddhi.debs2016.post.CommentPostMap;
import org.wso2.siddhi.debs2016.post.PostStore;
import org.wso2.siddhi.debs2016.util.Constants;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sarubi on 9/23/16.
 */
public class Q1ThreadsWorkLoadDistributed {
    private static final int BUFFERING_IN_READERS = 1024 * 1024 * 16;
    private static BufferedReader postReader;
    private static BufferedReader commentReader;
    private final static Splitter splitter = Splitter.on('|');
    private static final String MINUS_ONE = "-1";

    //Index 0 is Post. Index 1 is Comment
    private static Object[] debsEvents = new Object[2];
    public final CommentPostMap commentPostMap = new CommentPostMap();
    private final PostStore postStore = new PostStore();
    public static int noOfThreads;
    long[] count;


    public Q1ThreadsWorkLoadDistributed(String postsFile, String commentsFile, int NoOfThreads) {

        noOfThreads = NoOfThreads;
        count = new long[noOfThreads];

        try {
            postReader = new BufferedReader(new InputStreamReader(new FileInputStream(postsFile), "UTF-8"), Q1ThreadsWorkLoadDistributed.BUFFERING_IN_READERS);
            commentReader = new BufferedReader(new InputStreamReader(new FileInputStream(commentsFile), "UTF-8"), Q1ThreadsWorkLoadDistributed.BUFFERING_IN_READERS);
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

    public void startProcess() {


        LinkedBlockingQueue<Object[]>[] workloadQueue = new LinkedBlockingQueue[noOfThreads];
        for (int i = 0; i < noOfThreads; i++) {
            workloadQueue[i] = new LinkedBlockingQueue<>();
        }

        Thread[] threads = new Thread[noOfThreads];
        for (int i = 0; i < noOfThreads; i++) {
            threads[i] = new Thread(new Q1EventSingle(workloadQueue[i]));
        }

        int mod;
        boolean firstRound = true;


        while (true) {
            long lowestTimestamp = Long.MAX_VALUE;
            byte indexOfNextEvent = 0;
            Object[] nextEvent;
            String in = "";
            long postId;

            Object[] x = (Object[]) debsEvents[0];
            Object[] y = (Object[]) debsEvents[1];
            if (x[1].equals(Long.MAX_VALUE) && y[1].equals(Long.MAX_VALUE)) {
                Object[] lastObj = new Object[]{
                        0L,
                        -2L,
                        0L,
                        0L,
                        "",
                        "",
                        0L,
                        0L,
                        //-1
                        Constants.POSTS
                };
                for (int i = 0; i < noOfThreads; i++) {
                    workloadQueue[i].add(lastObj);
                }
                break;
            }


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

                        if (in != null && !in.isEmpty()) {
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
                            postId = (long) nextEvent[2];
                            postStore.addPost(postId, (Long) nextEvent[1], (String) nextEvent[5]);
                            mod = (int) (postId % noOfThreads);
                            workloadQueue[mod].add(nextEvent);
                            count[mod]++;
                            break;
                        } else {
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
                        postId = (long) nextEvent[2];
                        postStore.addPost(postId, (Long) nextEvent[1], (String) nextEvent[5]);
                        mod = (int) (postId % noOfThreads);
                        workloadQueue[mod].add(nextEvent);
                        count[mod]++;
                        break;

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

                            if ((long) nextEvent[7] == -1) {
                                long parent_post_id = commentPostMap.addCommentToComment((Long) nextEvent[3], (Long) nextEvent[6]);
                                mod = (int) (parent_post_id % noOfThreads);
                                workloadQueue[mod].add(nextEvent);
                                count[mod]++;
                                break;
                            } else {
                                postId = (long) nextEvent[7];
                                commentPostMap.addCommentToPost((Long) nextEvent[3], postId);
                                mod = (int) (postId % noOfThreads);
                                workloadQueue[mod].add(nextEvent);
                                count[mod]++;
                                break;
                            }
                        } else {
                            debsEvents[1] = new Object[]{
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

                        if ((long) nextEvent[7] == -1) {
                            long parent_post_id = commentPostMap.addCommentToComment((Long) nextEvent[3], (Long) nextEvent[6]);
                            mod = (int) (parent_post_id % noOfThreads);
                            workloadQueue[mod].add(nextEvent);
                            count[mod]++;
                            break;
                        } else {
                            postId = (long) nextEvent[7];
                            commentPostMap.addCommentToPost((Long) nextEvent[3], postId);
                            mod = (int) (postId % noOfThreads);
                            workloadQueue[mod].add(nextEvent);
                            count[mod]++;
                            break;
                        }
                }
                if (firstRound) {
                    firstRound = false;
                    for (int i = 0; i < noOfThreads; i++) {
                        threads[i].start();
                    }
                }


            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        long total = 0;
        for (int i = 0; i < noOfThreads; i++) {
            System.out.println("Q1 workload queue" + (i + 1) + " count: " + count[i]);
            total += count[i];
        }
        System.out.println("Q1 total event  count      : " + total);

    }


}
