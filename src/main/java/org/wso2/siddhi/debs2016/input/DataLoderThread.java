package org.wso2.siddhi.debs2016.input;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

public class DataLoderThread extends Thread {
    private String fileName;
    private static Splitter splitter = Splitter.on('|');
    private LinkedBlockingQueue<Object[]> eventBufferList;
    private BufferedReader br;
    private int count;
    private FileType typ;

    public DataLoderThread(String fileName, LinkedBlockingQueue<Object[]> eventBuffer, FileType fileType){
        super("Data Loader");
        this.fileName = fileName;
        this.eventBufferList = eventBuffer;
        this.typ = fileType;
    }

    public void run() {
        try {
            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
            String line = br.readLine();
            Object[] eventData = null;

            while (line != null) {
                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
                Iterator<String> dataStrIterator = splitter.split(line).iterator();

                String postsTimeStamp = dataStrIterator.next();
                Integer postID = Integer.parseInt(dataStrIterator.next());
                Integer userID = Integer.parseInt(dataStrIterator.next());
                String post = dataStrIterator.next();
                String user = dataStrIterator.next();

                eventData = new Object[]{
                        postsTimeStamp,
                        postID,
                        userID,
                        post,
                        user,
                        0l
                };

                eventBufferList.put(eventData);
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
