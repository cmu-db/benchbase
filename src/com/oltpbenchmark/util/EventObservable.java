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

import java.util.Observable;

/**
 * EventObservable
 * 
 */
public class EventObservable<T> {

    protected class InnerObservable extends Observable {
        @Override
        public synchronized void setChanged() {
            super.setChanged();
        }
        public EventObservable<T> getEventObservable() {
            return (EventObservable.this);
        }
    }
    
    private final InnerObservable observable;
    private int observer_ctr = 0;
    
    public EventObservable() {
        this.observable = new InnerObservable();
    }

    public synchronized void addObserver(EventObserver<T> o) {
        if (o != null) {
            this.observable.addObserver(o.getObserver());
            this.observer_ctr++;
        }
    }

    public synchronized void deleteObserver(EventObserver<T> o) {
        this.observable.deleteObserver(o.getObserver());
        this.observer_ctr--;
    }
    
    public synchronized void deleteObservers() {
        this.observable.deleteObservers();
        this.observer_ctr = 0;
    }
    
    public int countObservers() {
        return (this.observer_ctr);
    }

    /**
     * Notifies the Observers that a changed occurred
     * @param arg - the state that changed
     */
    public void notifyObservers(T arg) {
        this.observable.setChanged();
        if (this.observer_ctr > 0)
            this.observable.notifyObservers(arg);
    }

    /**
     * Notifies the Observers that a changed occurred
     */
    public void notifyObservers() {
        this.observable.setChanged();
        if (this.observer_ctr > 0)
            this.observable.notifyObservers();
    }
} // END CLASS