package com.oltpbenchmark;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.oltpbenchmark.types.State;
import com.oltpbenchmark.util.QueueLimitException;

/**
 * This class is used to share a state among the workers of a single
 * workload. Worker use it to ask for work and as interface to the global
 * BenchmarkState
 * @author alendit
 *
 */
public class WorkloadState {
    private static final int RATE_QUEUE_LIMIT = 10000;
    
    private BenchmarkState benchmarkState;
    private int workAvailable = 0;
    private int workersWaiting = 0;
    private int num_terminals;
    private int workerNeedSleep;
    
    private List<Phase> works = new ArrayList<Phase>();
    private Iterator<Phase> phaseIterator;
    private Phase currentPhase = null;
    
    public WorkloadState(BenchmarkState benchmarkState, List<Phase> works, int num_terminals) {
        this.benchmarkState = benchmarkState;
        this.works = works;
        this.num_terminals = num_terminals;
        
        phaseIterator = works.iterator();
    }
    
    /**
    * Add a request to do work.
    * 
    * @throws QueueLimitException
    */
   public void addToQueue(int amount, boolean resetQueues) throws QueueLimitException {
       assert amount > 0;
       if (resetQueues) {
           workAvailable = 0;
       }
       
       synchronized (this) {
           // don't increment workAvailable if the current phase is disabled
           // we still need to do it a last time when currentPhase is null
           if (currentPhase == null || !currentPhase.isDisabled()) {
               assert workAvailable >= 0;
    
               workAvailable += amount;
    
               if (workAvailable > RATE_QUEUE_LIMIT) {
                   // TODO: Deal with this appropriately. For now, we are
                   // ignoring it.
                   workAvailable = RATE_QUEUE_LIMIT;
                   // throw new QueueLimitException("Work queue limit ("
                   // + queueLimit
                   // + ") exceeded; Cannot keep up with desired rate");
               }
    
               if (workersWaiting <= amount) {
                   // Wake all waiting waiters
                   for (int i = 0; i < workersWaiting; i++) {
                       this.notify();
                   }
               } else {
                   // Only wake the correct number of waiters
                   assert workersWaiting > amount;
                   for (int i = 0; i < amount; ++i) {
                       this.notify();
                   }
               }
           }
       }
   }
   
   public void signalDone() {
       int current = this.benchmarkState.signalDone();
       if (current == 0) {
           synchronized (this) {
               if (workersWaiting > 0) {
                   this.notifyAll();
               }
           }
       }
   }
   
   /** Called by ThreadPoolThreads when waiting for work. */
   public State fetchWork() {
       synchronized (this) {
           if (workAvailable == 0) {
               workersWaiting += 1;
               while (workAvailable == 0) {
                   if (this.benchmarkState.getState() == State.EXIT) {
                       return State.EXIT;
                   }
                   try {
                       this.wait();
                   } catch (InterruptedException e) {
                       throw new RuntimeException(e);
                   }
               }
               workersWaiting -= 1;
           }

           assert workAvailable > 0;
           workAvailable -= 1;

           return this.benchmarkState.getState();
       }
   }
   
   public Phase getNextPhase() {
       if (phaseIterator.hasNext())
           return phaseIterator.next();
       return null;
   }
   
   public Phase getCurrentPhase() {
       synchronized (benchmarkState){
           return currentPhase;
       }
   }
   
   /*
    * Called by workers to ask if they should stay awake in this phase
    */
   public void stayAwake() {
       synchronized(this) {
           if (workerNeedSleep > 0 || (this.currentPhase != null && this.currentPhase.isDisabled())) {
               workerNeedSleep --;
               try {
                   this.wait();
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
           }
       }
       
   }
   
   public void switchToNextPhase() {
       synchronized(this) {
           boolean wakeUp = this.currentPhase != null &&
                   (this.currentPhase.isDisabled() || this.currentPhase.getActiveTerminals() < this.num_terminals);
           this.currentPhase = this.getNextPhase();
           if (wakeUp) {
               this.notifyAll();
           }
           if (this.currentPhase != null) {
               workerNeedSleep = this.num_terminals - this.currentPhase.getActiveTerminals();
           }
       }
   }
   
   /**
    * Delegates pre-start blocking to the global state handler
    */
   
   public void blockForStart() {
       benchmarkState.blockForStart();
   }
   
   /**
    * Delegates a global state query to the benchmark state handler
    * 
    * @return global state
    */
   public State getGlobalState() {
       return benchmarkState.getState();
   }
   
   public long getTestStartNs() {
       return benchmarkState.getTestStartNs();
   }
   
}
