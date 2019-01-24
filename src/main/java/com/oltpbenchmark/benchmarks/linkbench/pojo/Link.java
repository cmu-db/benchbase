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

/*
 * Copyright 2012, Facebook, Inc.
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
package com.oltpbenchmark.benchmarks.linkbench.pojo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import com.oltpbenchmark.benchmarks.linkbench.LinkBenchConstants;

public class Link {

  public Link(long id1, long link_type, long id2,
      byte visibility, byte[] data, int version, long time) {
    this.id1 = id1;
    this.link_type = link_type;
    this.id2 = id2;
    this.visibility = visibility;
    this.data = data;
    this.version = version;
    this.time = time;
  }

  public Link() {
    link_type = LinkBenchConstants.DEFAULT_LINK_TYPE;
    visibility = LinkBenchConstants.VISIBILITY_DEFAULT;
  }

  public boolean equals(Object other) {
    if (other instanceof Link) {
      Link o = (Link) other;
      return id1 == o.id1 && id2 == o.id2 &&
          link_type == o.link_type &&
          visibility == o.visibility &&
          version == o.version && time == o.time &&
          Arrays.equals(data, o.data);
    } else {
      return false;
    }
  }

  public String toString() {
    return String.format("Link(id1=%d, id2=%d, link_type=%d," +
        "visibility=%d, version=%d," +
        "time=%d, data=%s", id1, id2, link_type,
        visibility, version, time, data.toString());
  }

  /**
   * Clone an existing link
   * @param l
   */
  public Link clone() {
    Link l = new Link();
    l.id1 = this.id1;
    l.link_type = this.link_type;
    l.id2 = this.id2;
    l.visibility = this.visibility;
    l.data = this.data.clone();
    l.version = this.version;
    l.time = this.time;
    return l;
  }

  /** The node id of the source of directed edge */
  public long id1;

  /** The node id of the target of directed edge */
  public long id2;

  /** Type of link */
  public long link_type;

  /** Visibility mode */
  public byte visibility;

  /** Version of link */
  public int version;

  /** time is the sort key for links.  Often it contains a timestamp,
      but it can be used as a arbitrary user-defined sort key. */
  public long time;

  /** Arbitrary payload data */
  public byte[] data;
  
  public static Link createLinkFromRow(ResultSet rs) throws SQLException {
      Link l = new Link();
      l.id1 = rs.getLong(1);
      l.id2 = rs.getLong(2);
      l.link_type = rs.getLong(3);
      l.visibility = rs.getByte(4);
      l.data = rs.getBytes(5);
      l.time = rs.getLong(6);
      l.version = rs.getInt(7);
      return l;
    }

}
