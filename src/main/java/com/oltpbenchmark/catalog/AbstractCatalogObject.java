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

package com.oltpbenchmark.catalog;

import java.io.Serializable;
import java.util.Objects;

/**
 * Base Catalog Object Class
 *
 * @author pavlo
 */
public abstract class AbstractCatalogObject implements Serializable {
    static final long serialVersionUID = 0;

    protected final String name;
    protected final String separator;

    public AbstractCatalogObject(String name, String separator) {
        this.name = name;
        this.separator = separator;
    }

    public String getName() {
        return name;
    }

    public String getSeparator() {
        return separator;
    }

    public final String getEscapedName() {

        if (separator != null) {
            return separator + this.name + separator;
        } else {
            return this.name;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractCatalogObject that = (AbstractCatalogObject) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(separator, that.separator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, separator);
    }
}
