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

import java.util.HashMap;

public class CommentPostMap {

    public static final long DURATION = 86400000L;
    private final HashMap<Long, Long> commentToPostMap = new HashMap<>(5000);

    /**
     * Adding a comment to a post
     * @param commentId the comment id
     * @param postId the post id
     */
    public void addCommentToPost(Long commentId, Long postId){
        commentToPostMap.put(commentId, postId);
    }

    /**
     * Adding a comment to a comment
     * @param commentId the comment id
     * @param parentCommentId the parent comment id
     * @return ID of the parent post 
	 */
    public long addCommentToComment(Long commentId, Long parentCommentId){
        long parentPostId = commentToPostMap.get(parentCommentId);
        commentToPostMap.put(commentId, parentPostId);
        return parentPostId;
    }

    /**
     * Get the commentPostMap
     * @return commentPostMap
     */
    public HashMap<Long, Long> getCommentToPostMap() {
        return commentToPostMap;
    }
}
