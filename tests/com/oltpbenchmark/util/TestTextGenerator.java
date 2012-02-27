package com.oltpbenchmark.util;

import java.util.Arrays;
import java.util.Random;

import com.oltpbenchmark.util.TextGenerator;

import junit.framework.TestCase;

public class TestTextGenerator extends TestCase {

    final Random rng = new Random(); 
    
    /**
     * testRandomChars
     */
    public void testRandomChars() throws Exception {
        for (int i = 0; i < 1000; i++) {
            int strLen = rng.nextInt(2048);
            char text[] = TextGenerator.randomChars(rng, strLen);
            assertNotNull(text);
            assertEquals(strLen, text.length);
            
            for (int idx = 0; idx < strLen; idx++) {
                assertFalse(Integer.toString(idx), text[idx] == 0);
            } // FOR
        } // FOR
        
        // System.err.println(new String(text));
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
        int strLen = rng.nextInt(2048);
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
        System.err.println(new String(orig));
        System.err.println(new String(newText));
        assertTrue(valid);
        
    }
    
}
