package org.wso2.siddhi.debs2016.input;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingQueue;

public class DataLoderThread extends Thread {
    private String fileName;
    private static Splitter splitter = Splitter.on(',');
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
//        try {
//            br = new BufferedReader(new FileReader(fileName), 10 * 1024 * 1024);
//            String line = br.readLine();
//            while (line != null) {
//                //We make an assumption here that we do not get empty strings due to missing values that may present in the input data set.
//                Iterator<String> dataStrIterator = splitter.split(line).iterator();
//
//
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (Throwable e) {
//            e.printStackTrace();
//        }

        long startTime = System.currentTimeMillis();
        try {
            FileInputStream f = new FileInputStream(fileName);
            FileChannel ch = f.getChannel();
            MappedByteBuffer mb = ch.map(FileChannel.MapMode.READ_ONLY, 0L, ch.size());
            int size = 2000;
            byte comma = ',';
            byte newLine = '\n';

            byte[] barray = new byte[size];
            int lastLine = size;

            while (mb.hasRemaining()) {
                int copySize = size - lastLine;
                System.arraycopy(barray, lastLine, barray, 0, copySize);
                int dataToFetch = Math.min(mb.remaining(), size - copySize);
                mb.get(barray, copySize, dataToFetch);
                int nGet = copySize + dataToFetch;
                lastLine = 0;
                int last = 0;

                if(typ == FileType.POSTS) {
                    while (true) {
                        if (nGet < last + 106) {
                            break;
                        }

                        String ts = new String(barray, last, 32, StandardCharsets.UTF_8);
                        last += 33;

                        String post_id = new String(barray, last, 32, StandardCharsets.UTF_8);
                        last += 33;

                        String ts_last_recv = new String(barray, last, 32, StandardCharsets.UTF_8);
                    }
                }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
