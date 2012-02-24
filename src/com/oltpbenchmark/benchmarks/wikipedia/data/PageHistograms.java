package com.oltpbenchmark.benchmarks.wikipedia.data;

import com.oltpbenchmark.util.Histogram;

public abstract class PageHistograms {

    /**
     * The length of the PAGE_TITLE column
     */
    public static final Histogram<Integer> TITLE_LENGTH = new Histogram<Integer>() {
        {
            this.put(1, 5);
            this.put(2, 44);
            this.put(3, 364);
            this.put(4, 976);
            this.put(5, 1352);
            this.put(6, 2267);
            this.put(7, 2868);
            this.put(8, 3444);
            this.put(9, 3799);
            this.put(10, 4388);
            this.put(11, 5637);
            this.put(12, 7784);
            this.put(13, 9413);
            this.put(14, 7919);
            this.put(15, 5127);
            this.put(16, 3810);
            this.put(17, 3540);
            this.put(18, 3323);
            this.put(19, 2912);
            this.put(20, 2652);
            this.put(21, 2490);
            this.put(22, 2320);
            this.put(23, 2158);
            this.put(24, 1957);
            this.put(25, 1701);
            this.put(26, 1602);
            this.put(27, 1419);
            this.put(28, 1385);
            this.put(29, 1168);
            this.put(30, 1102);
            this.put(31, 1030);
            this.put(32, 984);
            this.put(33, 852);
            this.put(34, 801);
            this.put(35, 762);
            this.put(36, 639);
            this.put(37, 593);
            this.put(38, 531);
            this.put(39, 524);
            this.put(40, 472);
            this.put(41, 404);
            this.put(42, 353);
            this.put(43, 344);
            this.put(44, 307);
            this.put(45, 240);
            this.put(46, 250);
            this.put(47, 169);
            this.put(48, 195);
            this.put(49, 159);
            this.put(50, 130);
            this.put(51, 115);
            this.put(52, 124);
            this.put(53, 104);
            this.put(54, 78);
            this.put(55, 95);
            this.put(56, 77);
            this.put(57, 64);
            this.put(58, 66);
            this.put(59, 47);
            this.put(60, 75);
            this.put(61, 46);
            this.put(62, 45);
            this.put(63, 33);
            this.put(64, 39);
            this.put(65, 36);
            this.put(66, 30);
            this.put(67, 24);
            this.put(68, 28);
            this.put(69, 22);
            this.put(70, 13);
            this.put(71, 23);
            this.put(72, 15);
            this.put(73, 12);
            this.put(74, 11);
            this.put(75, 6);
            this.put(76, 12);
            this.put(77, 10);
            this.put(78, 7);
            this.put(79, 6);
            this.put(80, 7);
            this.put(81, 3);
            this.put(83, 4);
            this.put(84, 4);
            this.put(85, 2);
            this.put(86, 2);
            this.put(87, 4);
            this.put(88, 4);
            this.put(89, 1);
            this.put(90, 1);
            this.put(91, 5);
            this.put(92, 3);
            this.put(93, 6);
            this.put(94, 1);
            this.put(95, 3);
            this.put(96, 5);
            this.put(97, 1);
            this.put(99, 1);
            this.put(100, 1);
            this.put(103, 2);
            this.put(104, 1);
            this.put(105, 1);
            this.put(106, 2);
            this.put(109, 2);
            this.put(111, 1);
            this.put(115, 1);
            this.put(117, 1);
            this.put(118, 1);
            this.put(134, 1);
            this.put(141, 1);
        }
    };
    
    /**
     * The histogram of the PAGE_NAMESPACE column
     */
    public static final Histogram<Integer> NAMESPACE = new Histogram<Integer>() {
        {
            this.put(0, 40847);
            this.put(1, 15304);
            this.put(2, 4718);
            this.put(3, 23563);
            this.put(4, 2562);
            this.put(5, 268);
            this.put(6, 6991);
            this.put(7, 330);
            this.put(8, 9);
            this.put(9, 6);
            this.put(10, 1187);
            this.put(11, 263);
            this.put(12, 3);
            this.put(13, 2);
            this.put(14, 2831);
            this.put(15, 694);
            this.put(100, 393);
            this.put(101, 29);
        }
    };
    
    /**
     * The histogram of the PAGE_RESTRICTIONS column
     */
    public static final Histogram<String> RESTRICTIONS = new Histogram<String>() {
        {
            this.put("", 99917);
            this.put("edit=autoconfirmed:move=autoconfirmed", 20);
            this.put("edit=autoconfirmed:move=sysop", 8);
            this.put("edit=sysop:move=sysop", 23);
            this.put("move=:edit=", 24);
            this.put("move=sysop", 1);
            this.put("move=sysop:edit=sysop", 5);
            this.put("sysop", 2);
        }
    };
    
}
