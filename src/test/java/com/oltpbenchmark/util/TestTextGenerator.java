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

import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Random;

public class TestTextGenerator extends TestCase {

    final Random rng = new Random();
    final int MAX_SIZE = 2048;
    final int NUM_ROUNDS = 10000;

    /**
     * testRandomChars
     */
    public void testRandomChars() throws Exception {
        int strLen = rng.nextInt(MAX_SIZE) + 10;
        for (int i = 0; i < NUM_ROUNDS; i++) {
            // Make sure that the random strings are not null and
            // the length that they are supposed to be.
            char[] text = TextGenerator.randomChars(rng, strLen);
            assertNotNull(text);
            assertEquals(strLen, text.length);
        } // FOR
    }

    /**
     * testRandomCharsPrealloc
     */
    public void testRandomCharsPrealloc() throws Exception {
        int strLen = rng.nextInt(MAX_SIZE);
        char[] text = new char[strLen];
        for (int i = 0; i < NUM_ROUNDS; i++) {
            TextGenerator.randomChars(rng, text);
            assertNotNull(text);
            assertEquals(strLen, text.length);
        } // FOR
    }

    /**
     * testFastRandomChars
     */
    public void testFastRandomChars() throws Exception {
        for (int i = 0; i < NUM_ROUNDS; i++) {
            int strLen = rng.nextInt(MAX_SIZE) + 10;
            char[] text = new char[strLen];
            TextGenerator.randomFastChars(rng, text);
            assertNotNull(text);
            assertEquals(strLen, text.length);
        } // FOR
    }

    /**
     * testIncreaseText
     */
    public void testIncreaseText() throws Exception {
        int strLen = rng.nextInt(2048);
        char[] text = TextGenerator.randomChars(rng, strLen);
        assertNotNull(text);

        int delta = rng.nextInt(2048);
        char[] newText = TextGenerator.resizeText(rng, text, delta);
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
        char[] text = TextGenerator.randomChars(rng, strLen);
        assertNotNull(text);

        int delta = -1 * rng.nextInt(100);
        char[] newText = TextGenerator.resizeText(rng, text, delta);
        assertEquals(text.length + delta, newText.length);
    }

    /**
     * testPermuteText
     */
    public void testPermuteText() throws Exception {
        int strLen = 64; // rng.nextInt(64);
        char[] orig = TextGenerator.randomChars(rng, strLen);
        assertNotNull(orig);

        // Permute it and make sure at least one block is changed
        char[] newText = TextGenerator.permuteText(rng, Arrays.copyOf(orig, strLen));
        assertEquals(orig.length, newText.length);

        boolean valid = false;
        for (int i = 0; i < newText.length; i++) {
            if (orig[i] != newText[i]) {
                valid = true;
                break;
            }
        } // FOR
        assertTrue(valid);
    }

}
