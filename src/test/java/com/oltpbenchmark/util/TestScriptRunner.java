/*
 *  Copyright 2015 by OLTPBenchmark Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.oltpbenchmark.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class TestScriptRunner {

  @Test
  public void testNoDollarQuote() {
    assertNull(ScriptRunner.scanDollarQuote("CREATE TABLE foo (a int);", null));
    assertNull(ScriptRunner.scanDollarQuote("SELECT 'a $ b';", null));
  }

  @Test
  public void testOpensAndStaysOpen() {
    assertEquals("$$", ScriptRunner.scanDollarQuote("CREATE FUNCTION f() AS $$", null));
    assertEquals("$body$", ScriptRunner.scanDollarQuote("CREATE FUNCTION f() AS $body$", null));
    assertEquals("$$", ScriptRunner.scanDollarQuote("  x := 1; y := 2;", "$$"));
  }

  @Test
  public void testCloses() {
    assertNull(ScriptRunner.scanDollarQuote("$$;", "$$"));
    assertNull(ScriptRunner.scanDollarQuote("$body$ LANGUAGE plpgsql;", "$body$"));
    // a differently tagged quote does not close the open one
    assertEquals("$body$", ScriptRunner.scanDollarQuote("SELECT $$inner$$;", "$body$"));
  }

  @Test
  public void testOpensAndClosesOnOneLine() {
    assertNull(ScriptRunner.scanDollarQuote("CREATE FUNCTION f() AS $$ BEGIN END $$;", null));
    assertEquals(
        "$$", ScriptRunner.scanDollarQuote("SELECT $a$one$a$; CREATE FUNCTION f() AS $$", null));
  }
}
