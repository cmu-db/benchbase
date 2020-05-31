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


package com.oltpbenchmark.api;

public class LoaderUtil {

    /**
     * This is slow! Use TextGenerator.randomStr()
     *
     * @param strLen
     * @return
     */
    @Deprecated
    public static String randomStr(long strLen) {

        char freshChar;
        StringBuilder sb = new StringBuilder();

        while (sb.length() < (strLen - 1)) {
            freshChar = (char) (Math.random() * 128);
            if (Character.isLetter(freshChar)) {
                sb.append(freshChar);
            }
        }

        return sb.toString();

    }

}
