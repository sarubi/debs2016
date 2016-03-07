import org.junit.Test;
import org.wso2.siddhi.debs2016.input.DataLoaderThread;
import org.wso2.siddhi.debs2016.input.FileType;
import org.wso2.siddhi.debs2016.util.Constants;
import static org.junit.Assert.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by malithjayasinghe on 3/7/16.
 */
public class DataLoaderThreadTest {

    @Test
   public void test()
   {

       DataLoaderThread loaderTest = new DataLoaderThread("/Users/malithjayasinghe/debs2016/DataSet/data/posts.dat",
               new LinkedBlockingQueue<Object[]>(Constants.EVENT_BUFFER_SIZE), FileType.POSTS);
       loaderTest.start();
       assertTrue(true);

   }
}
