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
import java.util.Random;

/**
 * Fast Random Text Generator
 * @author pavlo
 */
public abstract class TextGenerator {
    
    private static final int CHAR_START = 32; // [space]
    private static final int CHAR_STOP  = 126; // [~]
    private static final char[] CHAR_SYMBOLS = new char[1 + CHAR_STOP - CHAR_START];
    static {
        for (int i = 0; i < CHAR_SYMBOLS.length; i++) {
            CHAR_SYMBOLS[i] = (char)(CHAR_START + i);
        } // FOR
    } // STATIC

    /**
     * Generate a random block of text as a char array
     * @param rng
     * @param strLen
     * @return
     */
    public static char[] randomChars(Random rng, int strLen) {
        char chars[] = new char[strLen];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char)CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
        } // FOR
        return (chars);
    }
    
    public static char[] randomChars(Random rng, char chars[], int start, int stop) {
        for (int i = start; i < stop; i++) {
            chars[i] = (char)CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
        } // FOR
        return (chars);
    }
    
    /**
     * Returns a new string filled with random text
     * @param rng
     * @param strLen
     * @return
     */
    public static String randomStr(Random rng, int strLen) {
        return new String(randomChars(rng, strLen));
    }
    
    /**
     * Returns a new string filled with random text. The first characters
     * of the string will be filled with the prefix
     * @param rng
     * @param strLen
     * @param prefix
     * @return
     */
    public static String randomStr(Random rng, int strLen, String prefix) {
        // Generate the random chars and then add in our prefix
        char chars[] = randomChars(rng, strLen);
        prefix.getChars(0, Math.min(prefix.length(), strLen), chars, 0);
        return new String(chars);
    }
    
    /**
     * Resize the given block of text by the delta and add random characters
     * to the new space in the array. Returns a new character array
     * @param rng
     * @param orig
     * @param delta
     * @return
     */
    public static char[] resizeText(Random rng, char orig[], int delta) {
        assert(orig.length + delta > 0) :
            String.format("Invalid resize (orig:%d, delta:%d)", orig.length, delta);
        char chars[] = Arrays.copyOf(orig, orig.length + delta);
        for (int i = orig.length; i < chars.length; i++) {
            chars[i] = (char)CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
        } // FOR
        return (chars);
    }
    
    /**
     * Permute a random portion of the origin text
     * Returns the same character array that was given as input
     * @param rng
     * @param chars
     * @return
     */
    public static char[] permuteText(Random rng, char chars[]) {
        // We will try to be fast about this and permute the text by blocks
        int idx = 0;
        int blockSize = chars.length / 32;
        
        // We'll generate one random number and check whether its bit is set to zero
        // Hopefully this is faster than having to generate a bunch of random
        // integers
        int rand = rng.nextInt();
        // If the number is zero, then flip one bit so that we make sure that 
        // we change at least one block
        if (rand == 0) rand = 1;
        for (int bit = 0; bit < 32; bit++) {
            if ((rand>>bit & 1) == 1) {
                for (int i = 0; i < blockSize; i++) {
                    chars[idx + i] = (char)CHAR_SYMBOLS[rng.nextInt(CHAR_SYMBOLS.length)];
                } // FOR
            }
            idx += blockSize;
            if (idx >= chars.length) break;
        } // FOR
        
        return (chars);
    }
    
}
