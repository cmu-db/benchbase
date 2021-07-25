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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author pavlo
 */
public abstract class FileUtil {

    private static final Pattern EXT_SPLIT = Pattern.compile("\\.");

    /**
     * Join path components
     *
     * @param args
     * @return
     */
    public static String joinPath(String... args) {
        StringBuilder result = new StringBuilder();
        boolean first = true;
        for (String a : args) {
            if (a != null && a.length() > 0) {
                if (!first) {
                    result.append("/");
                }
                result.append(a);
                first = false;
            }
        }
        return result.toString();
    }

    /**
     * Given a basename for a file, find the next possible filename if this file
     * already exists. For example, if the file test.res already exists, create
     * a file called, test.1.res
     *
     * @param basename
     * @return
     */
    public static String getNextFilename(String basename) {

        if (!exists(basename))
            return basename;

        File f = new File(basename);
        if (f != null && f.isFile()) {
            String parts[] = EXT_SPLIT.split(basename);

            // Check how many files already exist
            int counter = 1;
            String nextName = parts[0] + "." + counter + "." + parts[1];
            while (exists(nextName)) {
                ++counter;
                nextName = parts[0] + "." + counter + "." + parts[1];
            }
            return nextName;
        }

        // Should we throw instead??
        return null;
    }

    public static boolean exists(String path) {
        return (new File(path).exists());
    }

    /**
     * Create any directory in the list paths if it doesn't exist
     *
     * @param paths
     */
    public static void makeDirIfNotExists(String... paths) {
        for (String p : paths) {
            if (p == null) {
                continue;
            }
            File f = new File(p);
            if (!f.exists()) {
                f.mkdirs();
            }
        }
    }

    public static void writeStringToFile(File file, String content) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write(content);
            writer.flush();
        }
    }

}
