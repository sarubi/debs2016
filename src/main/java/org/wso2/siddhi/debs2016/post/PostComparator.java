package org.wso2.siddhi.debs2016.post;

import java.util.Comparator;

class PostComparator implements Comparator<Post> {

    @Override
    public int compare(Post postOne, Post postTwo) {
        Long timestampPostOne = postOne.getArrivalTime();
        Long timestampPostTwo = postTwo.getArrivalTime();


        if (timestampPostOne > timestampPostTwo) {
            return -1;
        } else if (timestampPostOne < timestampPostTwo) {
            return 1;
        } else {

            Long timestampCommentOne = postOne.getLatestCommentTime();
            Long timestampCommentTwo = postTwo.getLatestCommentTime();

            if (timestampCommentOne > timestampCommentTwo) {
                return -1;
            } else if (timestampCommentOne < timestampCommentTwo) {
                return 1;
            }
        }
        return 1;
    }
}