/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.auctionmark.util;

import java.util.Objects;

public class ItemCommentResponse {

    private final Long commentId;
    private final String itemId;
    private final String sellerId;

    public ItemCommentResponse(Long commentId, String itemId, String sellerId) {
        this.commentId = commentId;
        this.itemId = itemId;
        this.sellerId = sellerId;
    }

    public Long getCommentId() {
        return commentId;
    }

    public String getItemId() {
        return itemId;
    }

    public String getSellerId() {
        return sellerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ItemCommentResponse that = (ItemCommentResponse) o;
        return Objects.equals(commentId, that.commentId) && Objects.equals(itemId, that.itemId) && Objects.equals(sellerId, that.sellerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commentId, itemId, sellerId);
    }
}
