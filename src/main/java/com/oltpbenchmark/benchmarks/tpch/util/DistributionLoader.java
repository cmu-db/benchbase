/*
 * Copyright 2020 Trino
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
 */
package com.oltpbenchmark.benchmarks.tpch.util;

import com.oltpbenchmark.util.StringUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.stream.Stream;
import java.util.regex.Pattern;

public final class DistributionLoader {
    private DistributionLoader() {
    }

    public static <R extends Readable & Closeable> Map<String, Distribution> loadDistribution(Stream<String> lines)
            throws IOException {
        return loadDistributions(lines
                .map(String::trim)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .iterator());
    }

    private static Distribution loadDistribution(Iterator<String> lines, String name) {
        int count = -1;
        Map<String, Integer> members = new LinkedHashMap<>();
        while (lines.hasNext()) {
            // advance to "begin"
            String line = lines.next();
            if (isEnd(name, line)) {
                return new Distribution(name, members);
            }

            List<String> parts = StringUtil.splitToList(Pattern.compile("\\|"), line);

            String value = parts.get(0);
            int weight;
            try {
                weight = Integer.parseInt(parts.get(1));
            } catch (NumberFormatException e) {
                throw new IllegalStateException(
                        String.format("Invalid distribution %s: invalid weight on line %s", name, line));
            }

            if (value.equalsIgnoreCase("count")) {
                count = weight;
            } else {
                members.put(value, weight);
            }
        }
        throw new IllegalStateException(String.format("Invalid distribution %s: no end statement", name));
    }

    private static boolean isEnd(String name, String line) {
        List<String> parts = StringUtil.splitToList(StringUtil.WHITESPACE, line);
        if (parts.get(0).equalsIgnoreCase("END")) {
            return true;
        }
        return false;
    }

    private static Map<String, Distribution> loadDistributions(Iterator<String> lines) {
        Map<String, Distribution> distributions = new LinkedHashMap<>();
        while (lines.hasNext()) {
            // advance to "begin"
            String line = lines.next();
            List<String> parts = StringUtil.splitToList(StringUtil.WHITESPACE, line);
            if (parts.size() != 2) {
                continue;
            }

            if (parts.get(0).equalsIgnoreCase("BEGIN")) {
                String name = parts.get(1);
                Distribution distribution = loadDistribution(lines, name);
                distributions.put(name.toLowerCase(), distribution);
            }
        }
        return distributions;
    }
}
