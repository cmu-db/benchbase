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


package com.oltpbenchmark.benchmarks.auctionmark.util;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


public class CategoryParser {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryParser.class);

    Map<String, Category> _categoryMap;
    private int _nextCategoryID;
    String _fileName;

    public CategoryParser() {

        _categoryMap = new TreeMap<>();
        _nextCategoryID = 0;


        final String path = "/benchmarks/auctionmark/table.category";

        try (InputStream resourceAsStream = this.getClass().getResourceAsStream(path)) {

            List<String> lines = IOUtils.readLines(resourceAsStream, Charset.defaultCharset());
            for (String line : lines) {
                extractCategory(line);
            }

        } catch (Exception ex) {
            throw new RuntimeException("Failed to load in category file", ex);
        }

    }

    public void extractCategory(String s) {
        String[] tokens = s.split("\t");
        int itemCount = Integer.parseInt(tokens[5]);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i <= 4; i++) {
            if (!tokens[i].trim().isEmpty()) {
                sb.append(tokens[i].trim())
                        .append("/");
            } else {
                break;
            }
        }
        String categoryName = sb.toString();
        if (categoryName.length() > 0) {
            categoryName = categoryName.substring(0, categoryName.length() - 1);
        }

        addNewCategory(categoryName, itemCount, true);
    }

    public Category addNewCategory(String fullCategoryName, int itemCount, boolean isLeaf) {
        Category category = null;
        Category parentCategory = null;

        String categoryName = fullCategoryName;
        String parentCategoryName = "";
        Integer parentCategoryID = null;

        if (categoryName.indexOf('/') != -1) {
            int separatorIndex = fullCategoryName.lastIndexOf('/');
            parentCategoryName = fullCategoryName.substring(0, separatorIndex);
            categoryName = fullCategoryName.substring(separatorIndex + 1);
        }
		/*
		System.out.println("parentCat name = " + parentCategoryName);
		System.out.println("cat name = " + categoryName);
		*/
        if (_categoryMap.containsKey(parentCategoryName)) {
            parentCategory = _categoryMap.get(parentCategoryName);
        } else if (!parentCategoryName.isEmpty()) {
            parentCategory = addNewCategory(parentCategoryName, 0, false);
        }

        if (parentCategory != null) {
            parentCategoryID = parentCategory.getCategoryID();
        }

        category = new Category(_nextCategoryID++,
                categoryName,
                parentCategoryID,
                itemCount,
                isLeaf);

        _categoryMap.put(fullCategoryName, category);

        return category;
    }

    public Map<String, Category> getCategoryMap() {
        return _categoryMap;
    }

}
