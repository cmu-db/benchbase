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


package com.oltpbenchmark.benchmarks.wikipedia.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.util.FileUtil;

public class TransactionSelector {

    final Pattern p = Pattern.compile(" ");
    final Pattern clean = Pattern.compile("(.*)[ ]+\\-[ ]*$");
    
    final File file;
    BufferedReader reader = null;
    TransactionTypes transTypes;
    static final double READ_WRITE_RATIO = 11.8; // from
                                                 // http://www.globule.org/publi/WWADH_comnet2009.html
    
    public TransactionSelector(File file, TransactionTypes transTypes) throws FileNotFoundException {
        this.file = file;
        this.transTypes = transTypes;

        if (this.file == null)
            throw new FileNotFoundException("You must specify a filename to instantiate the TransactionSelector... (probably missing in your workload configuration?)");

        BufferedReader r = null;
        try {
            r = FileUtil.getReader(this.file);
        } catch (IOException ex) {
            throw new RuntimeException("Failed to open file '" + file + "' for reading", ex);
        }
        assert (r != null);
        this.reader = r;
    }

    public List<WikipediaOperation> readAll() throws IOException {
        ArrayList<WikipediaOperation> transactions = new ArrayList<WikipediaOperation>();
        while (this.reader.ready()) {
            String line = this.reader.readLine();
            String[] sa = p.split(line);

            int user = Integer.parseInt(sa[0]);
            int namespace = Integer.parseInt(sa[1]);

            int startIdx = sa[0].length() + sa[1].length() + 2;
            String title = line.substring(startIdx, startIdx + line.length() - startIdx);
            // HACK: Check whether they have a " - " at the end of the line
            // If they do, then that means that they are coming from a real
            // trace and we need to strip it out
            Matcher m = clean.matcher(title);
            if (m.find()) {
                title = m.group(1);
            }

            transactions.add(new WikipediaOperation(user, namespace, title.trim()));
        } // WHILE
        this.reader.close();
        return transactions;
    }

    public static void writeEntry(OutputStream out, int userId, int pageNamespace, String pageTitle) throws IOException {
        out.write(String.format("%d %d %s\n", userId, pageNamespace, pageTitle).getBytes());
    }
    public static void writeEntryDebug(OutputStream out, int userId, int pageNamespace, String pageTitle, int pageid) throws IOException {
        out.write(String.format("%d %d %s %d\n", userId, pageNamespace, pageTitle, pageid).getBytes());
    }
}
