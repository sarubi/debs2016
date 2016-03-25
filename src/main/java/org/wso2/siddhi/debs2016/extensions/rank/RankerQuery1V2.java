package org.wso2.siddhi.debs2016.extensions.rank;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.debs2016.input.CommentRecord;
import org.wso2.siddhi.debs2016.input.PostRecord;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by anoukh on 3/25/16.
 */
public class RankerQuery1V2 extends StreamFunctionProcessor {

    private long startiij_timestamp;
    private long endiij_timestamp;
    private long count;
    private Date startDateTime;
    long startTime = 0;

    @Override
    protected Object[] process(Object[] objects) {
        try {
            long iij_timestamp = (Long) objects[0];
            endiij_timestamp = iij_timestamp;
            long ts = (Long) objects[1];
            String user_name = (String) objects[5];
            int isPostFlag = (int) objects[8];

//            System.out.println(ts);

            if (ts == -1L) {
                //This is the place where time measuring starts.
                startiij_timestamp = iij_timestamp;
                return new Object[]{""};
            }

            if (ts == -2L) {
                //This is the place where time measuring ends.
                showFinalStatistics();
                return new Object[]{""};
            }
            count++;
            //For each incoming post or comment we have to add them to the appropriate data structure with their initial scores
            if (isPostFlag == 0) { //This is a new post
                long post_id = (Long) objects[2];
                long user_id = (Long) objects[3];

                return new Object[]{""};

            } else {
                long user_id = (Long) objects[2];
                long comment_id = (Long) objects[3];
                long comment_replied_id = (Long) objects[6];
                long post_replied_id = (Long) objects[7];
                return new Object[]{""};
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return objects;
    }

    @Override
    protected Object[] process(Object o) {
        return null;
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (expressionExecutors.length != 9) {
            System.err.println("Required Parameters : Nine");
            return null;
        }

        //We print the start and the end times of the experiment even if the performance logging is disabled.
        startDateTime = new Date();
        startTime = startDateTime.getTime();
        SimpleDateFormat ft = new SimpleDateFormat ("yyyy-MM-dd.hh:mm:ss-a-zzz");
//        System.out.println("Started experiment at : " + startTime + "--" + ft.format(startDateTime));

        System.out.println("Ranker Query 1 V2");
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(13);
        attributes.add(new Attribute("result", Attribute.Type.STRING));

        return attributes;
    }

    public void start() {

    }

    public void stop() {

    }

    public Object[] currentState() {
        return new Object[0];
    }

    public void restoreState(Object[] objects) {

    }

    private void showFinalStatistics()
    {
        long timeDifference = endiij_timestamp - startiij_timestamp;

        Date dNow = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd.hh:mm:ss-a-zzz");
        System.out.println("Ended experiment at : " + dNow.getTime() + "--" + ft.format(dNow));
        System.out.println("Event count : " + count);
        System.out.println("Total run time : " + timeDifference);
        System.out.println("Throughput (events/s): " + Math.round((count * 1000.0) / timeDifference));
        System.out.flush();
    }
}


