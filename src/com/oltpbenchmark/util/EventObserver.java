/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.util;

import java.util.*;

/**
 * EventObservable
 */
public abstract class EventObserver<T> {

    protected class InnerObserver implements Observer {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public void update(Observable o, Object arg) {
            assert(o instanceof EventObservable<?>.InnerObservable);
            EventObserver.this.update(((EventObservable.InnerObservable)o).getEventObservable(), (T)arg);
        }
        public EventObserver<T> getEventObserver() {
            return (EventObserver.this);
        }
    }
    
    private final InnerObserver observer;
    
    public EventObserver() {
        this.observer = new InnerObserver();
    }
    
    protected Observer getObserver() {
        return (this.observer);
    }

    public abstract void update(EventObservable<T> o, T arg);
}