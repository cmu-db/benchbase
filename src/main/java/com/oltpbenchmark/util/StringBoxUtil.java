package com.oltpbenchmark.util;

import java.util.regex.Pattern;

/**
 * Utility class for printing boxes around text
 * @author pavlo
 */
public abstract class StringBoxUtil {

    private static final Pattern LINE_SPLIT = Pattern.compile("\n");

    private static final String DEFAULT_MARKER = "*";
    
    public static final String UNICODE_BOX_CORNERS[] = {"\u250C", "\u2510", "\u2514", "\u2518"};
    public static final String UNICODE_BOX_VERTICAL = "\u2502";
    public static final String UNICODE_BOX_HORIZONTAL = "\u2500";
    
    public static final String UNICODE_HEAVYBOX_CORNERS[] = {"\u250F", "\u2513", "\u2517", "\u251B"};
    public static final String UNICODE_HEAVYBOX_VERTICAL = "\u2503";
    public static final String UNICODE_HEAVYBOX_HORIZONTAL = "\u2501";
    
    /**
     * 
     * @param str
     * @param horzMark
     * @param vertMark
     * @param max_len
     * @param corners
     * @return
     */
    public static String box(String str, String horzMark, String vertMark, Integer max_len, String corners[]) {
        String lines[] = LINE_SPLIT.split(str);
        if (lines.length == 0)
            return ("");
    
        // CORNERS: 
        //  0: Top-Left
        //  1: Top-Right
        //  2: Bottom-Left
        //  3: Bottom-Right
        if (corners == null) {
            corners = new String[]{horzMark, horzMark, horzMark, horzMark};
        }
        
        if (max_len == null) {
            for (String line : lines) {
                if (max_len == null || line.length() > max_len)
                    max_len = line.length();
            } // FOR
        }
    
        final String top_line = corners[0] + StringUtil.repeat(horzMark, max_len + 2) + corners[1]; // padding - two corners
        final String bot_line = corners[2] + StringUtil.repeat(horzMark, max_len + 2) + corners[3]; // padding - two corners
        final String f = "%s %-" + max_len + "s %s\n";
    
        StringBuilder sb = new StringBuilder();
        sb.append(top_line).append("\n");
        for (String line : lines) {
            sb.append(String.format(f, vertMark, line, vertMark));
        } // FOR
        sb.append(bot_line);
    
        return (sb.toString());
    }
    
    /**
     * Heavy unicode border box
     * @param str
     * @return
     */
    public static String heavyBox(String str) {
        return box(str, StringBoxUtil.UNICODE_HEAVYBOX_HORIZONTAL,
                        StringBoxUtil.UNICODE_HEAVYBOX_VERTICAL,
                        null,
                        StringBoxUtil.UNICODE_HEAVYBOX_CORNERS);
    }

    /**
     * Create a box around some text
     * 
     * @param str
     * @param mark
     * @param max_len
     * @return
     */
    public static String box(String str, String mark, Integer max_len) {
        return box(str, mark, mark, max_len, null);
    }

    /**
     * Make a box around some text using the given marker character.
     * 
     * @param str
     * @param mark
     * @return
     */
    public static String box(String str, String mark) {
        return (box(str, mark, null));
    }

    /**
     * Make a box around some text. If str has multiple lines, then the box will
     * be the length of the longest string.
     * 
     * @param str
     * @return
     */
    public static String box(String str) {
        return (box(str, DEFAULT_MARKER, null));
    }


}
