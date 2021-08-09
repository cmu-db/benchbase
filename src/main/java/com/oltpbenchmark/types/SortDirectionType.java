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

/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.oltpbenchmark.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public enum SortDirectionType {
    INVALID(0),
    ASC(1),
    DESC(2);

    SortDirectionType(int val) {
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, SortDirectionType> idx_lookup = new HashMap<>();
    protected static final Map<String, SortDirectionType> name_lookup = new HashMap<>();

    static {
        for (SortDirectionType vt : EnumSet.allOf(SortDirectionType.class)) {
            SortDirectionType.idx_lookup.put(vt.ordinal(), vt);
            SortDirectionType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static SortDirectionType get(Integer idx) {

        SortDirectionType ret = SortDirectionType.idx_lookup.get(idx);
        return (ret == null ? SortDirectionType.INVALID : ret);
    }

    public static SortDirectionType get(String name) {
        SortDirectionType ret = SortDirectionType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? SortDirectionType.INVALID : ret);
    }
}
