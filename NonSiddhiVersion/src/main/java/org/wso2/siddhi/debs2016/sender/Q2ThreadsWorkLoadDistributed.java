package org.wso2.siddhi.debs2016.sender;

import com.google.common.base.Splitter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.wso2.siddhi.debs2016.Processors.Q1EventSingle;
import org.wso2.siddhi.debs2016.Processors.Q2EventSingle;
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
public class Q2ThreadsWorkLoadDistributed {
    private BufferedReader friendshipsReader;
    private BufferedReader commentsReader;
    private BufferedReader likesReader;


    private final static int BUFFERING_IN_READERS = 1024 * 1024 * 16;
    private Object[] debsEvents = new Object[3];
    private final Splitter splitter = Splitter.on('|');
    private final String MINUS_ONE = "-1";

    long d;
    int k;
    int noOfThreads;
    int[] count;

    /**
     * The constructor
     */
    public Q2ThreadsWorkLoadDistributed(String friendshipFile, String commentsFile, String likesFile, long d, int k, int t) {

        this.d = d;
        this.k = k;
        this.noOfThreads = t;
        count = new int[noOfThreads];

        try {
            friendshipsReader = new BufferedReader(new InputStreamReader(new FileInputStream(friendshipFile), "UTF-8"), Q2ThreadsWorkLoadDistributed.BUFFERING_IN_READERS);
            commentsReader = new BufferedReader(new InputStreamReader(new FileInputStream(commentsFile), "UTF-8"), Q2ThreadsWorkLoadDistributed.BUFFERING_IN_READERS);
            likesReader = new BufferedReader(new InputStreamReader(new FileInputStream(likesFile), "UTF-8"), Q2ThreadsWorkLoadDistributed.BUFFERING_IN_READERS);
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
                long friendshipTimeStamp = friendshipDateTime.getMillis();
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
                long likeTimeStamp = likeDateTime.getMillis();
                long userID = Long.parseLong(likeStreamIterator.next());
                long commentID = Long.parseLong(likeStreamIterator.next());
                Object[] likeData = new Object[]{
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

    public void startProcess() {

        LinkedBlockingQueue<Object[]>[] workloadQueue = new LinkedBlockingQueue[noOfThreads];
        for (int i = 0; i < noOfThreads; i++) {
            workloadQueue[i] = new LinkedBlockingQueue<>();
        }

        Thread[] threads = new Thread[noOfThreads];
        for (int i = 0; i < noOfThreads; i++) {
            threads[i] = new Thread(new Q2EventSingle(workloadQueue[i], d, k));
        }

        int mod;
        boolean firstRound = true;

        while (true) {
            long lowestTimestamp = Long.MAX_VALUE;
            byte indexOfNextEvent = 0;
            Object[] nextEvent;
            String in = "";

            Object[] x = (Object[]) debsEvents[0];
            Object[] y = (Object[]) debsEvents[1];
            Object[] z = (Object[]) debsEvents[2];
            if (x[1].equals(Long.MAX_VALUE) && y[1].equals(Long.MAX_VALUE) && z[1].equals(Long.MAX_VALUE)) {
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
                        //Constants.POSTS
                        Constants.FRIENDSHIPS
                };
                for (int i = 0; i < noOfThreads; i++) {
                    workloadQueue[i].add(lastObj);
                }
                break;
            }

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

                        if (in != null && !in.isEmpty()) {
                            Iterator<String> friendshipStreamIterator = splitter.split(in).iterator();

                            String friendshipsTimeStampString = friendshipStreamIterator.next();
                            if (("").equals(friendshipsTimeStampString)) {
                                //TODO
                            } else {
                                DateTime friendshipDateTime = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC).parseDateTime(friendshipsTimeStampString);
                                long friendshipTimeStamp = friendshipDateTime.getMillis();
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
                            for (int i = 0; i < noOfThreads; i++) {
                                workloadQueue[i].add(nextEvent);
                                count[i]++;
                            }
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
                        for (int i = 0; i < noOfThreads; i++) {
                            workloadQueue[i].add(nextEvent);
                            count[i]++;
                        }
                        break;


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
                            mod = (int) ((long) nextEvent[3] % noOfThreads);
                            workloadQueue[mod].add(nextEvent);
                            count[mod]++;
                            break;

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
                        mod = (int) ((long) nextEvent[3] % noOfThreads);
                        workloadQueue[mod].add(nextEvent);
                        count[mod]++;
                        break;


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
                                long likeTimeStamp = likeDateTime.getMillis();
                                long userID = Long.parseLong(likeStreamIterator.next());
                                long commentID = Long.parseLong(likeStreamIterator.next());
                                Object[] likeData = new Object[]{
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
                            mod = (int) ((long) nextEvent[3] % noOfThreads);
                            workloadQueue[mod].add(nextEvent);
                            count[mod]++;
                            break;

                        } else {
                            debsEvents[2] = new Object[]{
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
                        mod = (int) ((long) nextEvent[3] % noOfThreads);
                        workloadQueue[mod].add(nextEvent);
                        count[mod]++;
                        break;

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
            System.out.println("Q2 workload queue" + (i + 1) + " count: " + count[i]);
            total += count[i];
        }
        System.out.println("Q2 total event  count : " + total);
    }
}
