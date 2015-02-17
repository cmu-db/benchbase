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

package com.oltpbenchmark.benchmarks.auctionmark.exceptions;

import com.oltpbenchmark.api.Procedure.UserAbortException;

public class DuplicateItemIdException extends UserAbortException {
    private static final long serialVersionUID = -667971163586760142L;
    
    private final long item_id;
    private final long seller_id;
    private final int item_count;
    
    public DuplicateItemIdException(long item_id, long seller_id, int item_count, Exception cause) {
        super(String.format("Duplicate ItemId #%d for Seller #%d", item_id, seller_id), cause);
        
        this.item_id = item_id;
        this.seller_id = seller_id;
        this.item_count = item_count;
    }
    
    public DuplicateItemIdException(long item_id, long seller_id, int item_count) {
        this(item_id, seller_id, item_count, null);
    }
    
    public long getItemId() {
        return this.item_id;
    }
    public long getSellerId() {
        return this.seller_id;
    }
    public int getItemCount() {
        return this.item_count;
    }
    
}
