/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.auctionmark.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.oltpbenchmark.util.Histogram;

import junit.framework.TestCase;

public class TestUserId extends TestCase {

    private static final Random rand = new Random(1);
    
    /**
     * testUserId
     */
    public void testUserId() {
        for (int i = 0; i < 100; i++) {
            int size = rand.nextInt(10000);
            for (int offset = 0; offset < 10; offset++) {
                UserId user_id = new UserId(size, offset);
                assertNotNull(user_id);
                assertEquals(size, user_id.getItemCount());
                assertEquals(offset, user_id.getOffset());
            } // FOR
        } // FOR
    }
    
    /**
     * testEquals
     */
    public void testEquals() {
        UserId user_id = new UserId(rand.nextLong());
        assert(user_id.getItemCount() > 0);
        assert(user_id.getOffset() > 0);
        
        UserId clone = new UserId(user_id.getItemCount(), user_id.getOffset());
        assertEquals(user_id, clone);
        assertEquals(user_id.hashCode(), clone.hashCode());
        assertEquals(0, user_id.compareTo(clone));
    }
    
    /**
     * testCompareTo
     */
    public void testCompareTo() throws Throwable {
        Histogram<UserId> h = new Histogram<UserId>();
        List<UserId> orig = new ArrayList<UserId>();
        
        Set<Long> seen_encode = new HashSet<Long>();
        Set<Integer> seen_hash = new HashSet<Integer>();
        Set<long[]> seen_array = new HashSet<long[]>();
        SortedMap<UserId, Boolean> seen_map = new TreeMap<UserId, Boolean>();
        
        int num_ids = 100;
        for (int i = 0; i < num_ids; i++) {
            UserId user_id = new UserId(rand.nextLong());
            assert(user_id.getItemCount() > 0);
            assert(user_id.getOffset() > 0);
            if (orig.contains(user_id)) {
                i--;
                continue;
            }
            
            System.err.println(String.format("[%02d] %-50s => %d / %d %s%s%s%s",
                    h.getValueCount(), user_id, user_id.hashCode(), user_id.encode(),
                    (seen_hash.contains(user_id.hashCode()) ? "!!! HASH" : ""),
                    (seen_encode.contains(user_id.encode()) ? "!!! ENCODE" : ""),
                    (seen_array.contains(user_id.toArray()) ? "!!! ARRAY" : ""),
                    (seen_map.containsKey(user_id) ? "!!! MAP" : "")
            ));
            
            h.put(user_id, i+1);
            assertTrue(user_id.toString(), h.contains(user_id));
            assertNotNull(user_id.toString(), h.get(user_id));
            assertEquals(user_id.toString(), i+1, h.get(user_id).intValue());
            assertEquals(i+1, h.getValueCount());
            
            orig.add(user_id);
            seen_hash.add(user_id.hashCode());
            seen_encode.add(user_id.encode());
            seen_array.add(user_id.toArray());
            seen_map.put(user_id, true);
        } // FOR
        assertEquals(num_ids, orig.size());
        assertEquals(num_ids, h.values().size());
        assertEquals(num_ids, h.getValueCount());
        assertEquals(num_ids, seen_hash.size());
        assertEquals(num_ids, seen_encode.size());
        
        for (int i = 0; i < num_ids; i++) {
            UserId user_id = orig.get(i);
            assertTrue(user_id.toString(), seen_encode.contains(user_id.encode()));
            assertTrue(user_id.toString(), seen_hash.contains(user_id.hashCode()));
            assertTrue(user_id.toString(), seen_map.containsKey(user_id));
            assertNotNull(user_id.toString(), h.get(user_id));
            assertEquals(user_id.toString(), i+1, h.get(user_id).intValue());
        }
        
        // Randomly delete a bunch and make sure that they're not in our histogram anymore
        Set<UserId> deleted = new HashSet<UserId>();
        for (int i = 0; i < num_ids; i++) {
            if (rand.nextBoolean()) {
                UserId user_id = orig.get(i);
                assertNotNull(user_id);
                h.removeAll(user_id);
                deleted.add(user_id);
            }
        } // FOR
        assertFalse(deleted.isEmpty());
        assertEquals(orig.size() - deleted.size(), h.getValueCount());
        for (UserId user_id : orig) {
            assertEquals(user_id.toString(), deleted.contains(user_id) == false, h.contains(user_id));
        } // FOR
        
    }
    
    /**
     * testUserIdEncode
     */
    public void testUserIdEncode() {
        for (int i = 0; i < 100; i++) {
            int size = rand.nextInt(10000);
            for (int offset = 0; offset < 10; offset++) {
                long encoded = new UserId(size, offset).encode();
                assert(encoded >= 0);
                
                UserId user_id = new UserId(encoded);
                assertNotNull(user_id);
                assertEquals(size, user_id.getItemCount());
                assertEquals(offset, user_id.getOffset());
            } // FOR
        } // FOR
    }
    
    /**
     * testUserIdDecode
     */
    public void testUserIdDecode() {
        for (int i = 0; i < 100; i++) {
            int size = rand.nextInt(10000);
            for (int offset = 0; offset < 10; offset++) {
                long values[] = { offset, size };
                long encoded = new UserId(size, offset).encode();
                assert(encoded >= 0);
                
                long new_values[] = new UserId(encoded).toArray();
                assertEquals(values.length, new_values.length);
                for (int j = 0; j < new_values.length; j++) {
                    assertEquals(values[j], new_values[j]);
                } // FOR
            } // FOR
        } // FOR
    }
}