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

import org.wso2.siddhi.debs2016.post.Post;

class TimeWindowComponent {
    private final long timestamp;
    private long expiringTime;
    private final Post post;
    private final long userId;
    private final boolean isPost;


    /**
     * Constructor to create TimeWindowComponent for Query1 TimeWidow
     *
     * @param post the post
     * @param timestamp the time stamp
     * @param userId the user ID of the user who commented on the post
     * @param isPost true if its a post object, false if is a comment object
     */
    public TimeWindowComponent(Post post, long timestamp, long userId, boolean isPost) {
        this.timestamp = timestamp;
        this.post = post;
        this.userId = userId;
        this.isPost = isPost;
        this.expiringTime = timestamp;

    }

    /**
     * Getter for userId variable
     * @return userId
     */
    public long getUserId() {
        return userId;
    }

    /**
     * Getter for isPost variable
     * @return true if object is Post object, false if it is Comment object
     */
    public boolean isPost() {
        return isPost;
    }

    /**
     * Getter for expiringTime variable
     * @return the time stamp of what time the object expires
     */
    public long getExpiringTime() {
        return expiringTime;
    }

    /**
     * Setter for expiringTime variable
     * @param expiringTime is the time that the object is expected to expire
     */
    public void setExpiringTime(long expiringTime) {
        this.expiringTime = expiringTime;
    }

    /**
     * Gets the post relating to he object
     * @return the post
     */
    public Post getPost() {
        return post;
    }

    /**
     * Gets the arrival time of the object
     * @return the comment arrival time
     */
    public long getTimestamp() {
        return timestamp;
    }

}
