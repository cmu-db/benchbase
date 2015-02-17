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

import com.oltpbenchmark.api.Operation;

/** Immutable class containing information about transactions. */
public final class WikipediaOperation extends Operation {
	
	public int userId;
	public final int nameSpace;
	public final String pageTitle;

	public WikipediaOperation(int userId, int nameSpace, String pageTitle) {
		// value of -1 indicate user is not logged in
		this.userId = userId;
		this.nameSpace = nameSpace;
		this.pageTitle = pageTitle;
	}
	
	@Override
	public String toString() {
	    return String.format("<UserId:%d, NameSpace:%d, Title:%s>",
	                         this.userId, this.nameSpace, this.pageTitle);
	}
}
