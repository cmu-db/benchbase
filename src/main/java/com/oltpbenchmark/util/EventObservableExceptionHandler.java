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


package com.oltpbenchmark.util;

public class EventObservableExceptionHandler extends EventObservable<Pair<Thread, Throwable>> implements Thread.UncaughtExceptionHandler {

    private Throwable error;
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        if (this.error == null) this.error = e;
        this.notifyObservers(Pair.of(t, e));
    }
    
    public boolean hasError() {
        return (this.error != null);
    }
    
    public Throwable getError() {
        return (this.error);
    }

}
