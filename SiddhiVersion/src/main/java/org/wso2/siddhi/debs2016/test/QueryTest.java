package org.wso2.siddhi.debs2016.test;

import org.wso2.siddhi.debs2016.query.Run;

/**
 * Created by bhagya on 4/22/16.
 */
public class QueryTest {
    private void testCase(String pathTofriendship,String pathToPost,String pathToComment,String pathToLike) {
        Run.main(new String[] {pathTofriendship, pathToPost, pathToComment, pathToLike, Integer.toString(2), Integer.toString(7200)});
    }
    public static void main(String[] args)  {
        String testCase=args[0];

        switch (testCase){
            case "1":
                new QueryTest().testCase("./data/1/friendships.dat","./data/1/posts.dat","./data/1/comments.dat","./data/1/likes.dat");
                break;
            case "2":
                new QueryTest().testCase("./data/2/friendships.dat","./data/2/posts.dat","./data/2/comments.dat","./data/2/likes.dat");
                break;
            case "3":
                new QueryTest().testCase("./data/3/friendships.dat","./data/3/posts.dat","./data/3/comments.dat","./data/3/likes.dat");
                break;
            case "4":
                new QueryTest().testCase("./data/4/friendships.dat","./data/4/posts.dat","./data/4/comments.dat","./data/4/likes.dat");
                break;
            case "5":
                new QueryTest().testCase("./data/5/friendships.dat","./data/5/posts.dat","./data/5/comments.dat","./data/5/likes.dat");
                break;
            case "6":
                new QueryTest().testCase("./data/6/friendships.dat","./data/6/posts.dat","./data/6/comments.dat","./data/6/likes.dat");
                break;
            case "7":
                new QueryTest().testCase("./data/7/friendships.dat","./data/7/posts.dat","./data/7/comments.dat","./data/7/likes.dat");
                break;
        }
    }
}
