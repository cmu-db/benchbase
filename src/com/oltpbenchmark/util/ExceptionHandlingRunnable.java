package com.oltpbenchmark.util;

public abstract class ExceptionHandlingRunnable implements Runnable {

    public abstract void runImpl();
    
    @Override
    public final void run() {
        try {
            this.runImpl();
        } catch (Throwable ex) {
            Thread t = Thread.currentThread();
            Thread.getDefaultUncaughtExceptionHandler().uncaughtException(t, ex);
        }
    }

}
