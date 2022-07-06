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

import java.util.Objects;

public class Category {
    private final int categoryID;
    private final Integer parentCategoryID;
    private final int itemCount;
    private final String name;
    private final boolean isLeaf;

    public Category(int categoryID, String name, Integer parentCategoryID, int itemCount, boolean isLeaf) {
        this.categoryID = categoryID;
        this.name = name;
        this.parentCategoryID = parentCategoryID;
        this.itemCount = itemCount;
        this.isLeaf = isLeaf;
    }

    public String getName() {
        return this.name;
    }

    public int getCategoryID() {
        return this.categoryID;
    }

    public Integer getParentCategoryID() {
        return this.parentCategoryID;
    }

    public int getItemCount() {
        return this.itemCount;
    }

    public boolean isLeaf() {
        return this.isLeaf;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Category category = (Category) o;
        return categoryID == category.categoryID && itemCount == category.itemCount && isLeaf == category.isLeaf && Objects.equals(parentCategoryID, category.parentCategoryID) && Objects.equals(name, category.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(categoryID, parentCategoryID, itemCount, name, isLeaf);
    }
}