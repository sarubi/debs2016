package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.debs2016.input.DataLoderThread;
import org.wso2.siddhi.debs2016.input.FileType;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by miyurud on 2/12/16.
 */
public class Query1 {
    public static void main(String[] args){
        Query1 query1 = new Query1(args);
        query1.run();
    }

    public Query1(String[] args){

    }

    public void run(){
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "@config(async = 'true')define stream postsStream (ts long, post_id long, user_id long, post string, user string);";
        String query = ("@info(name = 'query1') from postsStream#timeseries:kalmanMinMax(price, 0.000001,0.0001, 25, 'min')  " +
                "select price, extremaType, id " +
                "insert into outputStream;");

        LinkedBlockingQueue<Object[]> eventBufferList = new LinkedBlockingQueue<Object[]>();
        DataLoderThread dtThread = new DataLoderThread(eventBufferList, FileType.POSTS);
    }
}
