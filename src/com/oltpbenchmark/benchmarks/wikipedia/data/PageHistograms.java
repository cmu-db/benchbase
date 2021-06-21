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
     * Revisions per page
     * This seems way off because I think our sample data set is incomplete
     */
    public static final Histogram<Integer> REVISIONS_PER_PAGE = new Histogram<Integer>() {
        {
            this.put(1, 39401);  // XXX 39401
            this.put(2, 16869); // XXX 16869
            this.put(3, 8127);
            this.put(4, 5229);
            this.put(5, 3621);
            this.put(6, 2538);
            this.put(7, 2001);
            this.put(8, 1668);
            this.put(9, 1419);
            this.put(10, 1183);
            this.put(11, 1088);
            this.put(12, 981);
            this.put(13, 857);
            this.put(14, 771);
            this.put(15, 651);
            this.put(16, 637);
            this.put(17, 633);
            this.put(18, 537);
            this.put(19, 488);
            this.put(20, 500);
            this.put(21, 467);
            this.put(22, 394);
            this.put(23, 378);
            this.put(24, 354);
            this.put(25, 303);
            this.put(26, 304);
            this.put(27, 285);
            this.put(28, 249);
            this.put(29, 232);
            this.put(30, 258);
            this.put(31, 234);
            this.put(32, 205);
            this.put(33, 223);
            this.put(34, 171);
            this.put(35, 177);
            this.put(36, 170);
            this.put(37, 171);
            this.put(38, 155);
            this.put(39, 150);
            this.put(40, 129);
            this.put(41, 142);
            this.put(42, 120);
            this.put(43, 118);
            this.put(44, 129);
            this.put(45, 113);
            this.put(46, 92);
            this.put(47, 113);
            this.put(48, 98);
            this.put(49, 100);
            this.put(50, 76);
            this.put(51, 122);
            this.put(52, 89);
            this.put(53, 102);
            this.put(54, 84);
            this.put(55, 84);
            this.put(56, 78);
            this.put(57, 76);
            this.put(58, 71);
            this.put(59, 63);
            this.put(60, 69);
            this.put(61, 76);
            this.put(62, 60);
            this.put(63, 53);
            this.put(64, 56);
            this.put(65, 52);
            this.put(66, 47);
            this.put(67, 46);
            this.put(68, 46);
            this.put(69, 55);
            this.put(70, 46);
            this.put(71, 37);
            this.put(72, 50);
            this.put(73, 43);
            this.put(74, 43);
            this.put(75, 34);
            this.put(76, 46);
            this.put(77, 38);
            this.put(78, 37);
            this.put(79, 34);
            this.put(80, 49);
            this.put(81, 34);
            this.put(82, 33);
            this.put(83, 33);
            this.put(84, 40);
            this.put(85, 33);
            this.put(86, 28);
            this.put(87, 35);
            this.put(88, 29);
            this.put(89, 35);
            this.put(90, 20);
            this.put(91, 20);
            this.put(92, 35);
            this.put(93, 32);
            this.put(94, 27);
            this.put(95, 25);
            this.put(96, 25);
            this.put(97, 25);
            this.put(98, 28);
            this.put(99, 21);
            this.put(100, 244);
            this.put(110, 179);
            this.put(120, 167);
            this.put(130, 137);
            this.put(140, 98);
            this.put(150, 105);
            this.put(160, 88);
            this.put(170, 81);
            this.put(180, 72);
            this.put(190, 69);
            this.put(200, 62);
            this.put(210, 60);
            this.put(220, 38);
            this.put(230, 45);
            this.put(240, 43);
            this.put(250, 36);
            this.put(260, 38);
            this.put(270, 43);
            this.put(280, 36);
            this.put(290, 18);
            this.put(300, 33);
            this.put(310, 20);
            this.put(320, 18);
            this.put(330, 32);
            this.put(340, 19);
            this.put(350, 23);
            this.put(360, 27);
            this.put(370, 22);
            this.put(380, 17);
            this.put(390, 19);
            this.put(400, 9);
            this.put(410, 13);
            this.put(420, 12);
            this.put(430, 19);
            this.put(440, 16);
            this.put(450, 12);
            this.put(460, 10);
            this.put(470, 8);
            this.put(480, 5);
            this.put(490, 6);
            this.put(500, 7);
            this.put(510, 9);
            this.put(520, 9);
            this.put(530, 7);
            this.put(540, 12);
            this.put(550, 9);
            this.put(560, 8);
            this.put(570, 11);
            this.put(580, 4);
            this.put(590, 3);
            this.put(600, 12);
            this.put(610, 9);
            this.put(620, 5);
            this.put(630, 7);
            this.put(640, 5);
            this.put(650, 5);
            this.put(660, 4);
            this.put(670, 5);
            this.put(680, 2);
            this.put(690, 2);
            this.put(700, 4);
            this.put(710, 5);
            this.put(720, 4);
            this.put(730, 6);
            this.put(740, 7);
            this.put(750, 5);
            this.put(760, 2);
            this.put(770, 1);
            this.put(780, 2);
            this.put(800, 4);
            this.put(810, 1);
            this.put(820, 1);
            this.put(830, 1);
            this.put(840, 6);
            this.put(850, 3);
            this.put(860, 4);
            this.put(870, 1);
            this.put(880, 2);
            this.put(890, 4);
            this.put(900, 2);
            this.put(910, 2);
            this.put(920, 2);
            this.put(930, 1);
            this.put(940, 3);
            this.put(950, 6);
            this.put(960, 4);
            this.put(970, 1);
            this.put(980, 3);
            this.put(990, 2);
            this.put(1000, 1);
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
            // HACK: Normally MediaWiki just uses an empty field if there are no restrictions.
            //       But Oracle handles "" as NULL, so we need to make sure that our
            //       restrictions are never empty. 
            this.put("none", 99917); // HACK for Oracle
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
