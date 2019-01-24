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


package com.oltpbenchmark.catalog;

/**
 * 
 * @author Carlo A. Curino (carlo@curino.us)
 */
public class IntegrityConstraintsExistsException extends Exception {

    private static final long serialVersionUID = 1L;

    public IntegrityConstraintsExistsException() {
        // TODO Auto-generated constructor stub
    }

    public IntegrityConstraintsExistsException(String message) {
        super(message);
        // TODO Auto-generated constructor stub
    }

    public IntegrityConstraintsExistsException(Throwable cause) {
        super(cause);
        // TODO Auto-generated constructor stub
    }

    public IntegrityConstraintsExistsException(String message, Throwable cause) {
        super(message, cause);
        // TODO Auto-generated constructor stub
    }

}
