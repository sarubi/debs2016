package org.wso2.siddhi.debs2016.query;

class Run {

    public static void main(String[] args) {
        if(args.length != 6){
            System.err.println("Incorrect arguments. Required: <Path to>friendships.dat, <Path to>posts.dat, <Path to>comments.dat, <Path to>likes.dat, value for k, value for d");
        }else{
            Query1.main(args);
            Query2.main(args);
        }
    }
}
