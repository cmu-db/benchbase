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

public class GlobalAttributeValueId extends CompositeId implements Comparable<GlobalAttributeValueId> {

    private static final int[] COMPOSITE_BITS = {
            GlobalAttributeGroupId.ID_LENGTH, // GROUP_ATTRIBUTE_ID
            INT_MAX_DIGITS  // ID
    };

    private String group_attribute_id;
    private int id;

    public GlobalAttributeValueId(String group_attribute_id, int id) {
        this.group_attribute_id = group_attribute_id;
        this.id = id;
    }

    public GlobalAttributeValueId(GlobalAttributeGroupId group_attribute_id, int id) {
        this(group_attribute_id.encode(), id);
    }

    @Override
    public String encode() {
        return (super.encode(COMPOSITE_BITS));
    }

    @Override
    public void decode(String composite_id) {
        String[] values = super.decode(composite_id, COMPOSITE_BITS);
        this.group_attribute_id = values[0];
        this.id = Integer.parseInt(values[1]);
    }

    @Override
    public String[] toArray() {
        return (new String[]{this.group_attribute_id, Integer.toString(this.id)});
    }

    public GlobalAttributeGroupId getGlobalAttributeGroup() {
        return new GlobalAttributeGroupId(this.group_attribute_id);
    }

    protected int getId() {
        return (this.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GlobalAttributeValueId that = (GlobalAttributeValueId) o;
        return id == that.id && Objects.equals(group_attribute_id, that.group_attribute_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group_attribute_id, id);
    }

    @Override
    public int compareTo(GlobalAttributeValueId o) {
        return Comparator.comparing(GlobalAttributeValueId::getGlobalAttributeGroup)
                .thenComparingInt(GlobalAttributeValueId::getId)
                .compare(this, o);
    }
}
