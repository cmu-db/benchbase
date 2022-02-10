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

import com.oltpbenchmark.util.RowRandomInt;

import static com.oltpbenchmark.benchmarks.tpch.util.Distributions.getDefaultDistributions;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

public class TextPool {
    private static final int DEFAULT_TEXT_POOL_SIZE = 300 * 1024 * 1024;
    private static final int MAX_SENTENCE_LENGTH = 256;

    private static final TextPool DEFAULT_TEXT_POOL = new TextPool(DEFAULT_TEXT_POOL_SIZE, getDefaultDistributions());

    public static TextPool getDefaultTestPool() {
        return DEFAULT_TEXT_POOL;
    }

    private final byte[] textPool;
    private final int textPoolSize;

    public TextPool(int size, Distributions distributions) {
        this(size, distributions, progress -> {
        });
    }

    public TextPool(int size, Distributions distributions, TextGenerationProgressMonitor monitor) {
        requireNonNull(distributions, "distributions is null");
        requireNonNull(monitor, "monitor is null");

        ByteArrayBuilder output = new ByteArrayBuilder(size + MAX_SENTENCE_LENGTH);

        RowRandomInt randomInt = new RowRandomInt(933588178L, Integer.MAX_VALUE);

        while (output.getLength() < size) {
            generateSentence(distributions, output, randomInt);
            monitor.updateProgress(Math.min(1.0 * output.getLength() / size, 1.0));
        }
        output.erase(output.getLength() - size);
        textPool = output.getBytes();
        textPoolSize = output.getLength();
    }

    public int size() {
        return textPoolSize;
    }

    public String getText(int begin, int end) {
        if (end > textPoolSize) {
            throw new IndexOutOfBoundsException(
                    format("Index %d is beyond end of text pool (size = %d)", end, textPoolSize));
        }
        return new String(textPool, begin, end - begin, US_ASCII);
    }

    private static void generateSentence(Distributions distributions, ByteArrayBuilder builder, RowRandomInt random) {
        String syntax = distributions.getGrammars().randomValue(random);

        int maxLength = syntax.length();
        for (int i = 0; i < maxLength; i += 2) {
            switch (syntax.charAt(i)) {
                case 'V':
                    generateVerbPhrase(distributions, builder, random);
                    break;
                case 'N':
                    generateNounPhrase(distributions, builder, random);
                    break;
                case 'P':
                    String preposition = distributions.getPrepositions().randomValue(random);
                    builder.append(preposition);
                    builder.append(" the ");
                    generateNounPhrase(distributions, builder, random);
                    break;
                case 'T':
                    // trim trailing space
                    // terminators should abut previous word
                    builder.erase(1);
                    String terminator = distributions.getTerminators().randomValue(random);
                    builder.append(terminator);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown token '" + syntax.charAt(i) + "'");
            }
            if (builder.getLastChar() != ' ') {
                builder.append(" ");
            }
        }
    }

    private static void generateVerbPhrase(Distributions distributions, ByteArrayBuilder builder, RowRandomInt random) {
        String syntax = distributions.getVerbPhrase().randomValue(random);
        int maxLength = syntax.length();
        for (int i = 0; i < maxLength; i += 2) {
            Distribution source;
            switch (syntax.charAt(i)) {
                case 'D':
                    source = distributions.getAdverbs();
                    break;
                case 'V':
                    source = distributions.getVerbs();
                    break;
                case 'X':
                    source = distributions.getAuxiliaries();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown token '" + syntax.charAt(i) + "'");
            }

            // pick a random word
            String word = source.randomValue(random);
            builder.append(word);

            // add a space
            builder.append(" ");
        }
    }

    private static void generateNounPhrase(Distributions distributions, ByteArrayBuilder builder, RowRandomInt random) {
        String syntax = distributions.getNounPhrase().randomValue(random);
        int maxLength = syntax.length();
        for (int i = 0; i < maxLength; i++) {
            Distribution source;
            switch (syntax.charAt(i)) {
                case 'A':
                    source = distributions.getArticles();
                    break;
                case 'J':
                    source = distributions.getAdjectives();
                    break;
                case 'D':
                    source = distributions.getAdverbs();
                    break;
                case 'N':
                    source = distributions.getNouns();
                    break;
                case ',':
                    builder.erase(1);
                    builder.append(", ");
                    continue;
                case ' ':
                    continue;
                default:
                    throw new IllegalArgumentException("Unknown token '" + syntax.charAt(i) + "'");
            }
            // pick a random word
            String word = source.randomValue(random);
            builder.append(word);

            // add a space
            builder.append(" ");
        }
    }

    public interface TextGenerationProgressMonitor {
        void updateProgress(double progress);
    }

    private static class ByteArrayBuilder {
        private int length;
        private final byte[] bytes;

        public ByteArrayBuilder(int size) {
            this.bytes = new byte[size];
        }

        @SuppressWarnings("deprecation")
        public void append(String string) {
            // This is safe because the data is ASCII
            string.getBytes(0, string.length(), bytes, length);
            length += string.length();
        }

        public void erase(int count) {
            length -= count;
        }

        public int getLength() {
            return length;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public char getLastChar() {
            return (char) bytes[length - 1];
        }
    }
}
