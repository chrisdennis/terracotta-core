/*
 *  Copyright Terracotta, Inc.
 *  Copyright IBM Corp. 2024, 2025
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.tc.async.impl;

import org.slf4j.Logger;

import com.tc.async.api.ConfigurationContext;
import com.tc.async.api.EventHandler;
import com.tc.async.api.EventHandlerException;
import com.tc.async.api.MultiThreadedEventContext;
import com.tc.async.api.Sink;
import com.tc.async.api.Source;
import com.tc.async.api.Stage;
import com.tc.async.api.StageListener;
import com.tc.exception.TCNotRunningException;
import com.tc.exception.TCRuntimeException;
import com.tc.exception.TCServerRestartException;
import com.tc.exception.TCShutdownServerException;
import com.tc.logging.TCLoggerProvider;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;
import com.tc.util.concurrent.QueueFactory;
import com.tc.util.concurrent.ThreadUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.terracotta.tripwire.StageMonitor;
import org.terracotta.tripwire.TripwireFactory;

/**
 * The SEDA Stage
 */
public class StageImpl<EC> implements Stage<EC> {
  private static final long    pollTime = 3000; // This is the poor man's solution for
                                                // stage
  private final String         name;
  private final EventHandler<EC> handler;
  private final StageQueue<EC> stageQueue;
  private final WorkerThread[] threads;
  private final ThreadGroup    group;
  private final StageListener  listener;
  private final Logger logger;
  private final int            sleepMs;
  private final boolean        pausable;
  private volatile boolean     paused;
  private volatile boolean     shutdown = true;
  private final LongAdder  inflight = new LongAdder();
  private final long           warnStallTime = TCPropertiesImpl.getProperties()
                                                     .getLong(TCPropertiesConsts.L2_SEDA_STAGE_STALL_WARNING, 500);
  private volatile long lastWarnTime = 0;
  private int spinning = 0;
  
  private StageMonitor event;
  /**
   * The Constructor.
   * 
   * @param loggerProvider : logger
   * @param name : The stage name
   * @param type
   * @param handler : Event handler for this stage
   * @param queueCount : Number of threads and queues working on this stage with 1 thread bound to 1 queue
   * @param group : The thread group to be used
   * @param queueFactory : Factory used to create the queues
   * @param listener
   * @param queueSize : Max queue Size allowed
   */
  @SuppressWarnings("unchecked")
  public StageImpl(TCLoggerProvider loggerProvider, String name, Class<EC> type, EventHandler<EC> handler, int queueCount,
                   ThreadGroup group, QueueFactory queueFactory, StageListener listener, int queueSize, boolean canBeDirect, boolean stallLogging) {
    Objects.requireNonNull(handler);
    this.logger = loggerProvider.getLogger(Stage.class.getName() + ": " + name);
    this.name = name;
    if (queueCount > 1 && !MultiThreadedEventContext.class.isAssignableFrom(type)) {
      throw new IllegalArgumentException("the requested queue count is greater than one but the event type is not multi-threaded for stage:" + this.name);
    }
    this.threads = new WorkerThread[queueCount];
    this.handler = handler;
    this.stageQueue = StageQueue.FACTORY.factory(queueCount, queueFactory, type, eventCreator(canBeDirect), loggerProvider, name, queueSize);
    this.group = group;
    this.listener = listener;
    this.sleepMs = TCPropertiesImpl.getProperties().getInt("seda." + name + ".sleepMs", 0);
    if (this.sleepMs > 0) {
      logger.warn("Sleep of " + this.sleepMs + "ms enabled for stage " + name);
    }
    this.pausable = TCPropertiesImpl.getProperties().getBoolean("seda." + name + ".pausable", false);
    if (this.pausable) {
      logger.warn("Stage pausing is enabled for stage " + name);
    }
    this.event = TripwireFactory.createStageMonitor(name, queueCount);
    if (!stallLogging) {
      lastWarnTime = Long.MAX_VALUE;
    }
  }
  
  private EventCreator<EC> eventCreator(boolean direct) {
    return (direct) ? new DirectEventCreator<>(baseCreator(), ()->isEmpty()) : baseCreator();
  }
  
  private EventCreator<EC> baseCreator() {
    return (event) -> {
      long start = System.nanoTime();
      inflight.increment();
      return ()-> {
        long exec = System.nanoTime();
        if (exec - start > TimeUnit.MILLISECONDS.toNanos(warnStallTime)) {
          warnIfWarranted("queue", event, TimeUnit.NANOSECONDS.toMillis(exec-start));
        }
        try {
          handler.handleEvent(event);
          long end = System.nanoTime();;
          if (end - exec > TimeUnit.MILLISECONDS.toNanos(warnStallTime)) {
            warnIfWarranted("executed", event, TimeUnit.NANOSECONDS.toMillis(end-exec));
          }
        } finally {
          inflight.decrement();
        }
      };
    };
  }
  
  private void warnIfWarranted(String type, Object event, long time) {
    long now = System.currentTimeMillis();
    if (now - lastWarnTime > 1000) {
      lastWarnTime = now;
      logger.warn("Stage: {} has {} event {} for {}ms", name, type, event, time);
      if (listener != null) {
        listener.stageStalled(name, time, inflight.intValue());
      }
    }
  }
  
  @Override
  public boolean isEmpty() {
    return inflight.sum() == 0;
  }

  @Override
  public int size() {
    return inflight.intValue();
  }

  @Override
  public void setSpinningCount(int spin) {
    spinning = spin;
  }

  public void trackExtraStatistics(boolean enable) {
    stageQueue.enableAdditionalStatistics(enable);
  }

  @Override
  public void stop() {
    synchronized (this) {
      if (shutdown) {
        return;
      }
      shutdown = true;
    }
    stageQueue.close();
    event.unregister();
    stopThreads();
  }

  @Override
  public void destroy() {
    if (!shutdown) {
      this.stop();
      handler.destroy();
    }
  }

  @Override
  public void start(ConfigurationContext context) {
    synchronized (this) {
      if (!shutdown) {
        return;
      }
      shutdown = false;
    }
    handler.initializeContext(context);
    startThreads(context.getIdentifier());
    event.register();
  }

  @Override
  public Sink<EC> getSink() {
    return this.stageQueue;
  }

  @Override
  public int pause() {
    paused = true;
    return inflight.intValue();
  }

  @Override
  public void unpause() {
    paused = false;
  }
 
  private synchronized void startThreads(String contextId) {
    for (int i = 0; i < threads.length; i++) {
      String threadName = contextId != null ? contextId + " - " : "";
      threadName += "WorkerThread(" + name + ", " + i;
      if (threads.length > 1) {
        threadName = threadName + ", " + this.stageQueue.getSource(i).getSourceName() + ")";
      } else {
        threadName = threadName + ")";
      }
      threads[i] = new WorkerThread<>(threadName, this.stageQueue.getSource(i));
      threads[i].start();
    }
  }

  private synchronized void stopThreads() {
    for (WorkerThread thread : threads) {
      try {
        if (thread != null) {
          while (thread.isAlive()) {
            thread.join(10000);
            if (thread.isAlive()) {
              logger.warn("still waiting for {} to shutdown", thread.getName());
            }
          }
        }
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "StageImpl(" + name + ")";
  }
// for testing
  void waitForIdle() {
    Arrays.stream(threads).forEach(t->{
      while (!t.isIdle()) {
        ThreadUtil.reallySleep(500);
      }
    });
  }
  
  @Override
  public Map<String, ?> getState() {
    Map<String, Object> data = new LinkedHashMap<>();
    List<Object> tl = new ArrayList<>(threads.length);
    Arrays.stream(threads).forEach(t->{if (t != null) tl.add(t.getStats());});
    data.put("name", name);
    data.put("threadCount", threads.length);
    data.put("backlog", inflight.sum());
    data.put("sink", this.stageQueue.getState());
    data.put("threads", tl);
    return data;
  }

  private class WorkerThread<EC> extends Thread {
    private final Source       source;
    private volatile boolean idle = false;
    // these are single threaded, don't need special handling
    private long idleTime  = 0;
    private long runTime = 0;
    private long count = 0;

    public WorkerThread(String name, Source source) {
      super(group, name);
      setDaemon(true);
      this.source = source;
    }

    private void handleStageDebugPauses() {
      if (sleepMs > 0) {
        ThreadUtil.reallySleep(sleepMs);
      }
      while (paused || (pausable && "paused".equalsIgnoreCase(System.getProperty(name)))) {
        if (!paused) {
          logger.info("Stage paused, sleeping for 1s");
        }
        ThreadUtil.reallySleep(1000);
      }
    }
    
    public boolean isIdle() {
      return this.idle && this.source.isEmpty();
    }

    @Override
    public void run() {
      int spinCount = 0;
      boolean spinner = spinning > 0;
      while (!shutdown || !source.isEmpty()) {
        Event ctxt = null;
        try {
          long stopped = System.nanoTime();
          idle = true;
          ctxt = (spinner) ? source.poll(0) : source.poll(pollTime);
          if (ctxt != null) {
            idle = false;
            long running = System.nanoTime();
            handleStageDebugPauses();
            idleTime += (running - stopped);
            ctxt.call();
            long finishRun = System.nanoTime();
            runTime += (finishRun - running);
            count += 1;
            event.eventOccurred(size(), (finishRun - running));
            spinCount = 0;
            spinner = spinning > 0;
          } else {
            idleTime += (System.nanoTime() - stopped);
            if (spinCount++ >= spinning) {
              spinner = false;
            }
          }
        } catch (InterruptedException ie) {
          if (shutdown) { continue; }
          throw new TCRuntimeException(ie);
        } catch (EventHandlerException ie) {
          if (shutdown) { continue; }
          throw new TCRuntimeException(ie);
        } catch (TCServerRestartException restart) {
          throw restart;
        } catch (TCShutdownServerException shutdown) {
          throw shutdown;
        } catch (Exception e) {
          if (isTCNotRunningException(e)) {
            if (shutdown) { continue; }
            logger.info("Ignoring " + TCNotRunningException.class.getSimpleName() + " while handling context: "
                          + ctxt);
          } else {
            logger.error("Uncaught exception in stage", e);
            throw new TCRuntimeException("Uncaught exception in stage", e);
          }
        } finally {
          // Aggressively null out the reference before going around the loop again. If you don't do this, the reference
          // to the context will exist until another context comes in. This can potentially keep many objects in memory
          // longer than necessary
          ctxt = null;
        }
      }
    }
    
    private Map<String, ?> getStats() {
      Map<String, Object> state = new LinkedHashMap<>();
      state.put("idle", idleTime);
      state.put("run", runTime);
      state.put("processed", count);
      state.put("backlog", source.size());
      return state;
    }
  }
  
  

  private static boolean isTCNotRunningException(Throwable e) {
    while (e != null) {
      if (e instanceof TCNotRunningException) {
        return true;
      } else {
        e = e.getCause();
      }
    }
    return false;
  }

  @Override
  public boolean isStarted() {
    return !shutdown;
  }
}
