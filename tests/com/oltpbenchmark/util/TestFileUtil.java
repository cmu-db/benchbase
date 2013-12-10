package com.oltpbenchmark.util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Time;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileUtil {

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
        } catch (IOException e) {}
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
