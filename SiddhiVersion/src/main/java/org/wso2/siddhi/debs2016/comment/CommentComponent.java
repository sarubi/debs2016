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

package org.wso2.siddhi.debs2016.comment;

class CommentComponent {
    private final long timestamp;
    private final long commentId;

    /**
     * Constructor to create commentComponent object for Query2
     * @param timestamp is time of arrival of the comment
     * @param commentId is the ID of the comment
     */
    public CommentComponent(long timestamp, long commentId) {
        this.timestamp = timestamp;
        this.commentId = commentId;
    }

    /**
     * Get the comment ID of the component
     * @return commentId
     */
    public long getCommentId() {
        return commentId;
    }

    /**
     * Get the time of arrival of the comment
     * @return timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

}