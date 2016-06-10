/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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