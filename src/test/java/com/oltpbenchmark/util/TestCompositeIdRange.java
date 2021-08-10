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

import static org.junit.Assert.*;

import org.junit.Test;

import junit.framework.TestCase;

public class TestCompositeIdRange {

    public class PackedLong extends CompositeId {
        protected final int COMPOSITE_BITS[] = {
                16, // FIELD1
                32, // FIELD2
        };
        protected final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);

        protected int field1;
        protected int field2;

        public PackedLong() {
        }

        public PackedLong(int field1, int field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public long encode() {
            return (this.encode(this.COMPOSITE_BITS, this.COMPOSITE_POWS));
        }

        @Override
        public void decode(long composite_id) {
            long values[] = super.decode(composite_id, this.COMPOSITE_BITS, this.COMPOSITE_POWS);
            this.field1 = (int) values[0];
            this.field2 = (int) values[1];
        }

        @Override
        public long[] toArray() {
            return (new long[]{this.field1, this.field2});
        }

        public int getField1() {
            return this.field1;
        }

        public int getField2() {
            return this.field2;
        }
    }

    @Test
    public void testPackOK() {
        PackedLong packedLong = new PackedLong(1, 2);
        long encodedLong = packedLong.encode();
        PackedLong packedLong2 = new PackedLong();
        packedLong2.decode(encodedLong);
        assertEquals(packedLong.getField1(), packedLong2.getField1());
        assertEquals(packedLong.getField2(), packedLong2.getField2());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOutOfRange() {
        // Won't fit, we have allocated only 16 bits to field 1
        PackedLong packedLong = new PackedLong(Integer.MAX_VALUE, Integer.MAX_VALUE);
        long encodedLong = packedLong.encode();
    }

}