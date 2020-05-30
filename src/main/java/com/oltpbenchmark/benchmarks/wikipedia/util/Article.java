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


package com.oltpbenchmark.benchmarks.wikipedia.util;

public class Article {

    public String userText;
    public int pageId;
    public String oldText;
    public long textId;
    public long revisionId;

    public Article(String userText, int pageId, String oldText, long textId,
                   long revisionId) {
        super();
        this.userText = userText;
        this.pageId = pageId;
        this.oldText = oldText;
        this.textId = textId;
        this.revisionId = revisionId;
    }

}
