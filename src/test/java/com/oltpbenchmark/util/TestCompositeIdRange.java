/******************************************************************************
 *  Copyright 2021 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.util;

import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class TestCompositeIdRange {

    public static class PackedLong extends CompositeId {
        private static final int[] COMPOSITE_BITS = {
                INT_MAX_DIGITS, // FIELD1
                LONG_MAX_DIGITS, // FIELD2
        };

        protected int field1;
        protected long field2;

        public PackedLong() {
        }

        public PackedLong(int field1, long field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public String encode() {
            return (this.encode(COMPOSITE_BITS));
        }

        @Override
        public void decode(String composite_id) {
            String[] values = super.decode(composite_id, COMPOSITE_BITS);
            this.field1 = Integer.parseInt(values[0]);
            this.field2 = Long.parseLong(values[1]);
        }

        @Override
        public String[] toArray() {
            return (new String[]{Integer.toString(this.field1), Long.toString(this.field2)});
        }

        public int getField1() {
            return this.field1;
        }

        public long getField2() {
            return this.field2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PackedLong that = (PackedLong) o;
            return field1 == that.field1 && field2 == that.field2;
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2);
        }
    }

    @Test
    public void testPackOK() {
        PackedLong packedLong = new PackedLong(1, 2);
        String encodedLong = packedLong.encode();
        PackedLong packedLong2 = new PackedLong();
        packedLong2.decode(encodedLong);
        assertEquals(packedLong.getField1(), packedLong2.getField1());
        assertEquals(packedLong.getField2(), packedLong2.getField2());
    }

}