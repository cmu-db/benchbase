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

package com.oltpbenchmark.benchmarks.twitter.util;

import com.oltpbenchmark.util.Histogram;

/**
 * A histogram of tweet length. This is derived from
 * http://simplymeasured.com/blog/2010/06/lakers-vs-celtics-social-media-breakdown-nba/
 * <p>
 * And seems to match the distribution shown in:
 * http://blog.hubspot.com/Portals/249/sotwitter09.pdf
 *
 * @author pavlo
 */
public class TweetHistogram extends Histogram<Integer> {

    {
        this.put(4, 2);
        this.put(6, 610);
        this.put(7, 2253);
        this.put(8, 1488);
        this.put(9, 1656);
        this.put(10, 2837);
        this.put(11, 3040);
        this.put(12, 2865);
        this.put(13, 2850);
        this.put(14, 3372);
        this.put(15, 4639);
        this.put(16, 5023);
        this.put(17, 4787);
        this.put(18, 4669);
        this.put(19, 4703);
        this.put(20, 4470);
        this.put(21, 4417);
        this.put(22, 4307);
        this.put(23, 4484);
        this.put(24, 4636);
        this.put(25, 4691);
        this.put(26, 4865);
        this.put(27, 5181);
        this.put(28, 5122);
        this.put(29, 5043);
        this.put(30, 5143);
        this.put(31, 5165);
        this.put(32, 5362);
        this.put(33, 5342);
        this.put(34, 5255);
        this.put(35, 5499);
        this.put(36, 5336);
        this.put(37, 5164);
        this.put(38, 5445);
        this.put(39, 5220);
        this.put(40, 5114);
        this.put(41, 5144);
        this.put(42, 5120);
        this.put(43, 5007);
        this.put(44, 5256);
        this.put(45, 5158);
        this.put(46, 5314);
        this.put(47, 5085);
        this.put(48, 5288);
        this.put(49, 5095);
        this.put(50, 5113);
        this.put(51, 4954);
        this.put(52, 4932);
        this.put(53, 5024);
        this.put(54, 4779);
        this.put(55, 5169);
        this.put(56, 4742);
        this.put(57, 4916);
        this.put(58, 5180);
        this.put(59, 4791);
        this.put(60, 4552);
        this.put(61, 4548);
        this.put(62, 4547);
        this.put(63, 4536);
        this.put(64, 4557);
        this.put(65, 4407);
        this.put(66, 4350);
        this.put(67, 4217);
        this.put(68, 4443);
        this.put(69, 4145);
        this.put(70, 4211);
        this.put(71, 4148);
        this.put(72, 3980);
        this.put(73, 4079);
        this.put(74, 3956);
        this.put(75, 4151);
        this.put(76, 3920);
        this.put(77, 3655);
        this.put(78, 3655);
        this.put(79, 3887);
        this.put(80, 3777);
        this.put(81, 3551);
        this.put(82, 3693);
        this.put(83, 3622);
        this.put(84, 3470);
        this.put(85, 3610);
        this.put(86, 4867);
        this.put(87, 4690);
        this.put(88, 3344);
        this.put(89, 3278);
        this.put(90, 3303);
        this.put(91, 3260);
        this.put(92, 3117);
        this.put(93, 3129);
        this.put(94, 3148);
        this.put(95, 3037);
        this.put(96, 3087);
        this.put(97, 2976);
        this.put(98, 2970);
        this.put(99, 2874);
        this.put(100, 2879);
        this.put(101, 2991);
        this.put(102, 2832);
        this.put(103, 2706);
        this.put(104, 2684);
        this.put(105, 2870);
        this.put(106, 2749);
        this.put(107, 2639);
        this.put(108, 2496);
        this.put(109, 2585);
        this.put(110, 2640);
        this.put(111, 2787);
        this.put(112, 2602);
        this.put(113, 2497);
        this.put(114, 2495);
        this.put(115, 2586);
        this.put(116, 2523);
        this.put(117, 2641);
        this.put(118, 2574);
        this.put(119, 2833);
        this.put(120, 2321);
        this.put(121, 2261);
        this.put(122, 2429);
        this.put(123, 2440);
        this.put(124, 2419);
        this.put(125, 2367);
        this.put(126, 2536);
        this.put(127, 2588);
        this.put(128, 2602);
        this.put(129, 2628);
        this.put(130, 2750);
        this.put(131, 2778);
        this.put(132, 2741);
        this.put(133, 2973);
        this.put(134, 3545);
        this.put(135, 4364);
        this.put(136, 4371);
        this.put(137, 4436);
        this.put(138, 6212);
        this.put(139, 6919);
        this.put(140, 15701);
    }

}
