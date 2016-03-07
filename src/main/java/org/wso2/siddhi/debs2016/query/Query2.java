package org.wso2.siddhi.debs2016.query;


public class Query2 {
    private String dataSetFolder;

    public static void main(String[] args){
        if(args.length != 1){
            System.err.println("Usage java org.wso2.siddhi.debs2016.query.Query2 <full path to data set folder>");
            return;
        }

        Query2 query2 = new Query2(args);
        query2.run();
    }

    public Query2(String[] args){
        dataSetFolder = args[0];
    }

    public void run(){

    }
}
