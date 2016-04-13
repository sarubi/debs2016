package org.wso2.siddhi.debs2016.query;

import org.wso2.siddhi.debs2016.sender.OrderedEventSenderThreadQ1;

/**
 * Created by anoukh on 3/16/16.
 */
public class Run {

    public static String [] INPUT_ARGUMENTS;
    public static void main(String[] args) {
        if(args.length != 6){
            System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat, value for k, value for d");
            return;
        }else{

           // Query1  query1 = new Query1(args);
            INPUT_ARGUMENTS = args;
            Query1.main(args);


        }
    }
}
