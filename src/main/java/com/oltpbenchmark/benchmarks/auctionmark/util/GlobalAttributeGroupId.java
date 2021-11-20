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

import com.oltpbenchmark.util.CompositeId;

import java.util.Comparator;
import java.util.Objects;
import java.util.stream.IntStream;

public class GlobalAttributeGroupId extends CompositeId implements Comparable<GlobalAttributeGroupId> {

    private static final int[] COMPOSITE_BITS = {
            INT_MAX_DIGITS, // CATEGORY
            INT_MAX_DIGITS,  // ID
            INT_MAX_DIGITS   // COUNT
    };

    public static final int ID_LENGTH = IntStream.of(COMPOSITE_BITS).sum();

    private int category_id;
    private int id;
    private int count;

    public GlobalAttributeGroupId(int category_id, int id, int count) {
        this.category_id = category_id;
        this.id = id;
        this.count = count;
    }

    public GlobalAttributeGroupId(String composite_id) {
        this.decode(composite_id);
    }

    @Override
    public String encode() {
        return (this.encode(COMPOSITE_BITS));
    }

    @Override
    public void decode(String composite_id) {
        String[] values = super.decode(composite_id, COMPOSITE_BITS);
        this.category_id = Integer.parseInt(values[0]);
        this.id = Integer.parseInt(values[1]);
        this.count = Integer.parseInt(values[2]);
    }

    @Override
    public String[] toArray() {
        return (new String[]{Integer.toString(this.category_id), Integer.toString(this.id), Integer.toString(this.count)});
    }

    public int getCategoryId() {
        return (this.category_id);
    }

    protected int getId() {
        return (this.id);
    }

    public int getCount() {
        return (this.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GlobalAttributeGroupId that = (GlobalAttributeGroupId) o;
        return category_id == that.category_id && id == that.id && count == that.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(category_id, id, count);
    }

    @Override
    public int compareTo(GlobalAttributeGroupId o) {
        return Comparator.comparingInt(GlobalAttributeGroupId::getCategoryId)
                .thenComparingInt(GlobalAttributeGroupId::getId)
                .thenComparingInt(GlobalAttributeGroupId::getCount)
                .compare(this, o);
    }
}
