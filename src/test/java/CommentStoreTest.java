import org.junit.Test;
import org.wso2.siddhi.debs2016.comment.CommentStore;
import org.wso2.siddhi.debs2016.graph.Graph;

import static org.junit.Assert.*;

public class CommentStoreTest {
    @Test
    public void testUpdateCommentStoreData1(){
        CommentStore comment = new CommentStore(10, new Graph(), 1);
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


        comment.cleanCommentStore(111);
        assertEquals(10,comment.getNumberOfComments());

    }

    @Test
    public void testUpdateCommentStoreData2(){
        CommentStore comment = new CommentStore(1, new Graph() , 1);
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


        comment.cleanCommentStore(111);
        assertEquals(0,comment.getNumberOfComments());

    }

@Test
    public void testGetKLargestComments(){
        CommentStore theStore = new CommentStore(100, new Graph(), 8);

        theStore.registerComment(1,100,"Comment One");
        theStore.registerComment(2,101,"Comment Two");
        theStore.registerComment(3,102,"Comment Three");
        theStore.registerComment(4,103,"Comment Four");
        theStore.registerComment(5,104,"Comment Five");
        theStore.registerComment(6,105,"Comment Six");

        theStore.registerLike(1,1);
        theStore.registerLike(1,2);
        theStore.registerLike(1,3);
        theStore.registerLike(1,4);
        theStore.registerLike(1,5);

        theStore.registerLike(2,8);
        theStore.registerLike(2,9);
        theStore.registerLike(2,10);
        theStore.registerLike(2,100);
        theStore.registerLike(2,105);
        theStore.registerLike(2,110);

        theStore.registerLike(3,13);
        theStore.registerLike(3,24);
        theStore.registerLike(3,15);
        theStore.registerLike(3,17);
        theStore.registerLike(3,19);
        theStore.registerLike(3,20);
        theStore.registerLike(3,200);

        theStore.registerLike(4,31);
        theStore.registerLike(4,51);
        theStore.registerLike(4,61);
        theStore.registerLike(4,47);

        theStore.handleNewFriendship(1,2);
        theStore.handleNewFriendship(1,5);
        theStore.handleNewFriendship(1,3);
        theStore.handleNewFriendship(5,4);
        theStore.handleNewFriendship(8,10);
        theStore.handleNewFriendship(10,9);
        theStore.handleNewFriendship(13,20);
        theStore.handleNewFriendship(24,20);
        theStore.handleNewFriendship(19,15);
        theStore.handleNewFriendship(15,17);
        theStore.handleNewFriendship(31,47);
        theStore.handleNewFriendship(47,51);
        theStore.handleNewFriendship(20,19);
        theStore.handleNewFriendship(8,100);
        theStore.handleNewFriendship(100,105);
        theStore.handleNewFriendship(8,110);
        theStore.computeKLargestComments(",", false, false);
    }
}
