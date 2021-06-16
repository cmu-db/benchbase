/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.oltpbenchmark.util.TextGenerator;

import junit.framework.TestCase;

public class TestTextGenerator extends TestCase {

    final Random rng = new Random(); 
    
    final int MAX_SIZE = 2048;
    final int NUM_ROUNDS = 10000;
    boolean SPEED_TEST = false;
    
    /**
     * testRandomChars
     */
    public void testRandomChars() throws Exception {
        Set<String> previous = new HashSet<String>();
        long start = System.nanoTime();
        int strLen = rng.nextInt(MAX_SIZE) + 10;
        for (int i = 0; i < NUM_ROUNDS; i++) {
            char text[] = TextGenerator.randomChars(rng, strLen);
            if (SPEED_TEST == false) {
                assertNotNull(text);
                assertEquals(strLen, text.length);
                for (int idx = 0; idx < strLen; idx++) {
                    assertFalse(Integer.toString(idx), text[idx] == 0);
                } // FOR
            }
            assertFalse(String.valueOf(text), previous.contains(String.valueOf(text)));
            previous.add(String.valueOf(text));
        } // FOR
        long stop = System.nanoTime();
        assertEquals(NUM_ROUNDS, previous.size());
        System.err.println("Chars Time: " + ((stop - start) / 1000000d) + " ms");
    }
    
    /**
     * testRandomCharsPrealloc
     */
    public void testRandomCharsPrealloc() throws Exception {
        Set<String> previous = new HashSet<String>();
        long start = System.nanoTime();
        int strLen = rng.nextInt(MAX_SIZE);
        char text[] = new char[strLen];
        for (int i = 0; i < NUM_ROUNDS; i++) {
            TextGenerator.randomChars(rng, text);
            if (SPEED_TEST == false) {
                assertNotNull(text);
                assertEquals(strLen, text.length);
                for (int idx = 0; idx < strLen; idx++) {
                    assertFalse(Integer.toString(idx), text[idx] == 0);
                } // FOR
            }
            assertFalse(String.valueOf(text), previous.contains(String.valueOf(text)));
            previous.add(String.valueOf(text));
        } // FOR
        long stop = System.nanoTime();
        assertEquals(NUM_ROUNDS, previous.size());
        System.err.println("Pre-allocated Time: " + ((stop - start) / 1000000d) + " ms");
    }
    
    /**
     * testFastRandomChars
     */
    public void testFastRandomChars() throws Exception {
        Set<String> previous = new HashSet<String>();
        long start = System.nanoTime();
        for (int i = 0; i < NUM_ROUNDS; i++) {
            int strLen = rng.nextInt(MAX_SIZE) + 10;
            char text[] = new char[strLen];
            TextGenerator.randomFastChars(rng, text);
//            System.err.println("FAST: " + String.valueOf(text));
            if (SPEED_TEST == false) {
                assertNotNull(text);
                assertEquals(strLen, text.length);
                for (int idx = 0; idx < strLen; idx++) {
                    assertFalse(Integer.toString(idx), text[idx] == 0);
                } // FOR
            }
            assertFalse("Duplicate: " + String.valueOf(text),
                        previous.contains(String.valueOf(text)));
            previous.add(String.valueOf(text));
        } // FOR
        long stop = System.nanoTime();
        assertEquals(NUM_ROUNDS, previous.size());
        System.err.println("Fast Time: " + ((stop - start) / 1000000d) + " ms");
    }
    
    /**
     * testIncreaseText
     */
    public void testIncreaseText() throws Exception {
        int strLen = rng.nextInt(2048);
        char text[] = TextGenerator.randomChars(rng, strLen);
        assertNotNull(text);
        
        int delta = rng.nextInt(2048);
        char newText[] = TextGenerator.resizeText(rng, text, delta);
        assertEquals(text.length + delta, newText.length);
        
        // Make sure the first portion is the same
        for (int i = 0; i < text.length; i++) {
            assertEquals(text[i], newText[i]);
        } // FOR
    }
    
    /**
     * testDecreaseText
     */
    public void testDecreaseText() throws Exception {
        // Make sure that the original length is always greater than the delta size
        int strLen = rng.nextInt(2048) + 200;
        char text[] = TextGenerator.randomChars(rng, strLen);
        assertNotNull(text);
        
        int delta = -1 * rng.nextInt(100);
        char newText[] = TextGenerator.resizeText(rng, text, delta);
        assertEquals(text.length + delta, newText.length);
    }
    
    /**
     * testPermuteText
     */
    public void testPermuteText() throws Exception {
        int strLen = 64; // rng.nextInt(64);
        char orig[] = TextGenerator.randomChars(rng, strLen);
        assertNotNull(orig);
        
        // Permute it and make sure at least one block is changed
        char newText[] = TextGenerator.permuteText(rng, Arrays.copyOf(orig, strLen));
        assertEquals(orig.length, newText.length);
        
        boolean valid = false;
        for (int i = 0; i < newText.length; i++) {
            if (orig[i] != newText[i]) {
                valid = true;
                break;
            }
        } // FOR
//        System.err.println(new String(orig));
//        System.err.println(new String(newText));
        assertTrue(valid);
        
    }
    
}
