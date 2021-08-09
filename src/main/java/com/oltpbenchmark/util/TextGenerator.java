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

package com.oltpbenchmark.util;

import java.util.Arrays;
import java.util.Random;

/**
 * Fast Random Text Generator
 *
 * @author pavlo
 */
public abstract class TextGenerator {

    private static final int CHAR_START = 32; // [space]
    private static final int CHAR_STOP = 126; // [~]
    private static final char[] CHAR_SYMBOLS = new char[1 + CHAR_STOP - CHAR_START];

    static {
        for (int i = 0; i < CHAR_SYMBOLS.length; i++) {
            CHAR_SYMBOLS[i] = (char) (CHAR_START + i);
        }
    }

    private static final int[] FAST_MASKS = {
            554189328, // 10000
            277094664, // 01000
            138547332, // 00100
            69273666,  // 00010
            34636833,  // 00001
            346368330, // 01010
            727373493, // 10101
            588826161, // 10001
            935194491, // 11011
            658099827, // 10011
    };


    /**
     * Generate a random block of text as a char array
     *
     * @param rng
     * @param strLen
     * @return
     */
    public static char[] randomChars(Random rng, int strLen) {
        char[] chars = new char[strLen];
        return randomFastChars(rng, chars);
    }

    public static char[] randomChars(Random rng, char[] chars) {
        for (int i = 0; i < chars.length; i++) {
            chars[i] = CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
        } // FOR
        return (chars);
    }

    /**
     * Faster (pseudo) random number generator
     *
     * @param rng
     * @param chars
     * @return
     */
    public static char[] randomFastChars(Random rng, char[] chars) {
        // Ok so now the goal of this is to reduce the number of times that we have to 
        // invoke a random number. We'll do this by grabbing a single random int
        // and then taking different bitmasks

        int num_rounds = chars.length / FAST_MASKS.length;
        int i = 0;
        for (int ctr = 0; ctr < num_rounds; ctr++) {
            int rand = rng.nextInt(10000); // CHAR_SYMBOLS.length);
            for (int mask : FAST_MASKS) {
                chars[i++] = CHAR_SYMBOLS[(rand | mask) % CHAR_SYMBOLS.length];
            }
        }
        // Use the old way for the remaining characters
        // I am doing this because I am too lazy to think of something more clever
        for (; i < chars.length; i++) {
            chars[i] = CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
        }
        return (chars);
    }

    /**
     * Returns a new string filled with random text
     *
     * @param rng
     * @param strLen
     * @return
     */
    public static String randomStr(Random rng, int strLen) {
        return new String(randomChars(rng, strLen));
    }

    /**
     * Resize the given block of text by the delta and add random characters
     * to the new space in the array. Returns a new character array
     *
     * @param rng
     * @param orig
     * @param delta
     * @return
     */
    public static char[] resizeText(Random rng, char[] orig, int delta) {
        char[] chars = Arrays.copyOf(orig, orig.length + delta);
        for (int i = orig.length; i < chars.length; i++) {
            chars[i] = CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
        }
        return (chars);
    }

    /**
     * Permute a random portion of the origin text
     * Returns the same character array that was given as input
     *
     * @param rng
     * @param chars
     * @return
     */
    public static char[] permuteText(Random rng, char[] chars) {
        // We will try to be fast about this and permute the text by blocks
        int idx = 0;
        int blockSize = chars.length / 32;

        // We'll generate one random number and check whether its bit is set to zero
        // Hopefully this is faster than having to generate a bunch of random
        // integers
        int rand = rng.nextInt();
        // If the number is zero, then flip one bit so that we make sure that 
        // we change at least one block
        if (rand == 0) {
            rand = 1;
        }
        for (int bit = 0; bit < 32; bit++) {
            if ((rand >> bit & 1) == 1) {
                for (int i = 0; i < blockSize; i++) {
                    chars[idx + i] = CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
                }
            }
            idx += blockSize;
            if (idx >= chars.length) {
                break;
            }
        }

        return (chars);
    }

}
