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

package com.oltpbenchmark.util;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestFileUtil extends TestCase {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    void touch(String name) {
        try {
            File f = new File(name);
            if (!f.exists())
                new FileOutputStream(f).close();
            f.setLastModified(TimeUtil.getCurrentTime().getTime());
        } catch (IOException e) {
        }
    }

    void rm(String name) {
        File file = new File(name);
        file.delete();
    }

    @Test
    public void testIncrementFileNames() {

        String basename = "base.res";
        assertEquals("base.res", FileUtil.getNextFilename(basename));
        touch("base.res");
        assertEquals("base.1.res", FileUtil.getNextFilename(basename));
        assertEquals("base.1.res", FileUtil.getNextFilename(basename));
        touch("base.1.res");
        assertEquals("base.2.res", FileUtil.getNextFilename(basename));


        rm("base.res");
        rm("base.1.res");
        rm("base.2.res");
    }

}
