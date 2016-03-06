package org.wso2.siddhi.debs2016.input;

import org.wso2.siddhi.debs2016.util.Constants;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by malithjayasinghe on 3/6/16.
 * TO DO: move this class to test folder. Not sure why the test folder is missing in this project
 * TO DO: remove main and implement like a unit test
 */
public class DataLoaderThreadTest {

    public static void main(String args[])
    {
        DataLoaderThread loaderTest = new DataLoaderThread("/Users/malithjayasinghe/debs2016/DataSet/data/posts.dat",
                new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE), FileType.POSTS);
        loaderTest.start();

    }

}
