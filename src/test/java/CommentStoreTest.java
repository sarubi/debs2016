import org.junit.Test;
import org.wso2.siddhi.debs2016.comment.CommentStore;

import static org.junit.Assert.assertEquals;

/**
 * Created by bhagya on 3/11/16.
 */
public class CommentStoreTest {
    @Test
    public void testUpdateCommentStoreData1(){
        CommentStore comment = new CommentStore(10);
        comment.registerComment(101,100,"hi");
        comment.registerComment(102,101,"hi");
        comment.registerComment(103,102,"hi");
        comment.registerComment(104,103,"hi");
        comment.registerComment(105,104,"hi");
        comment.registerComment(106,95,"hi");
        comment.registerComment(107,106,"hi");
        comment.registerComment(108,107,"hi");
        comment.registerComment(109,108,"hi");
        comment.registerComment(110,109,"hi");
        comment.registerComment(111,110,"hi");


        comment.updateCommentStore(111);
        assertEquals(9,comment.getNumberOfComments());

    }
    @Test
    public void testUpdateCommentStoreData2(){
        CommentStore comment = new CommentStore(1);
        comment.registerComment(101,100,"hi");
        comment.registerComment(102,101,"hi");
        comment.registerComment(103,102,"hi");
        comment.registerComment(104,103,"hi");
        comment.registerComment(105,104,"hi");
        comment.registerComment(106,105,"hi");
        comment.registerComment(107,106,"hi");
        comment.registerComment(108,107,"hi");
        comment.registerComment(109,108,"hi");
        comment.registerComment(110,109,"hi");


        comment.updateCommentStore(111);
        assertEquals(0,comment.getNumberOfComments());

    }
}
