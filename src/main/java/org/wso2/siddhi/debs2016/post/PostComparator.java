package org.wso2.siddhi.debs2016.post;

import java.util.Comparator;

/**
 * Created by malithjayasinghe on 4/6/16.
 *
 * Compare the two posts
 */
public class PostComparator implements Comparator<Post> {

    @Override
    public int compare(Post post_1, Post post_2) {
        Long ts_1 = post_1.getArrivalTime();
        Long ts_2 = post_2.getArrivalTime();


        if (ts_1 > ts_2) {
            return -1;
        } else if (ts_1 < ts_2) {
            return 1;
        } else {

            Long ts_comment_1 = post_1.getLatestCommentTime();
            Long ts_comment_2 = post_2.getLatestCommentTime();

            if (ts_comment_1 > ts_comment_2) {
                return -1;
            } else if (ts_comment_1 < ts_comment_2) {
                return 1;
            }
        }
        return 1;
    }
}