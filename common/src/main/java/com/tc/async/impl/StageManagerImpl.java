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
import com.tc.async.api.PostInit;
import com.tc.async.api.Stage;
import com.tc.async.api.StageListener;
import com.tc.async.api.StageManager;
import com.tc.logging.DefaultLoggerProvider;
import com.tc.logging.TCLoggerProvider;
import com.tc.properties.TCPropertiesConsts;
import com.tc.properties.TCPropertiesImpl;
import com.tc.stats.Stats;
import com.tc.text.MapListPrettyPrint;
import com.tc.util.Assert;
import com.tc.util.concurrent.QueueFactory;
import com.tc.util.concurrent.ThreadUtil;
import java.util.ArrayList;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author steve
 */
public class StageManagerImpl implements StageManager {
  private static final int         DEFAULT_MAX_STAGE_CAPACITY = TCPropertiesImpl.getProperties()
                                                     .getInt(TCPropertiesConsts.L2_SEDA_STAGE_SINK_CAPACITY);
  private static final boolean     MONITOR       = TCPropertiesImpl.getProperties()
                                                     .getBoolean(TCPropertiesConsts.TC_STAGE_MONITOR_ENABLED);
  private static final long        MONITOR_DELAY = TCPropertiesImpl.getProperties()
                                                     .getLong(TCPropertiesConsts.TC_STAGE_MONITOR_DELAY);

  private final Map<String, Stage<?>>   stages        = new ConcurrentHashMap<>();
  private final Map<String, Class<?>> classVerifications = new ConcurrentHashMap<>();
  private TCLoggerProvider           loggerProvider = new DefaultLoggerProvider();
  private final ThreadGroup          group;
  private String[]                   stageNames    = new String[] {};
  private final QueueFactory queueFactory;
  private final StageListener listener;
  private volatile boolean           started;

  public StageManagerImpl(ThreadGroup threadGroup, QueueFactory queueFactory) {
    this.loggerProvider = new DefaultLoggerProvider();
    this.group = threadGroup;
    this.queueFactory = queueFactory;
    this.listener = new NullStageListener();

    if (MONITOR) {
      startMonitor();
    }
  }

  public StageManagerImpl(ThreadGroup threadGroup, QueueFactory queueFactory, StageListener listener) {
    this.loggerProvider = new DefaultLoggerProvider();
    this.group = threadGroup;
    this.queueFactory = queueFactory;
    this.listener = listener;

    if (MONITOR) {
      startMonitor();
    }
  }
  
  private void startMonitor() {
    final Logger logger = loggerProvider.getLogger(getClass());
    Thread t = new Thread(group, "SEDA Stage Monitor") {
      @Override
      public void run() {
        while (true) {
          printStats();
          ThreadUtil.reallySleep(MONITOR_DELAY);
        }
      }

      private void printStats() {
        try {
          Stats stats[] = StageManagerImpl.this.getStats();
          logger.info("Stage Depths");
          logger.info("=================================");
          for (Stats stat : stats) {
            stat.logDetails(logger);
          }
        } catch (Throwable th) {
          logger.error("Exception :", th);
        }
      }
    };
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void setLoggerProvider(TCLoggerProvider loggerProvider) {
    this.loggerProvider = loggerProvider;
  }

  @Override
  public <EC> Stage<EC> createStage(String name, Class<EC> verification, EventHandler<EC> handler, int threads) {
    return this.createStage(name, verification, handler, threads, DEFAULT_MAX_STAGE_CAPACITY, false, true);
  }

  @Override
  public synchronized <EC> Stage<EC> createStage(String name, Class<EC> verification, EventHandler<EC> handler, int queueCount, int maxSize, boolean canBeDirect, boolean stallWarn) {
    if (started) {
      throw new IllegalStateException("A new stage cannot be created, because StageManager is already started.");
    }
    // Note that the queue factory is used by all the stages under this manager so it can't be type-safe.
    Stage<EC> s = new StageImpl<>(loggerProvider, name, verification, handler, queueCount, group, queueFactory, listener, maxSize, canBeDirect, stallWarn);
    addStage(name, s);
    this.classVerifications.put(name,  verification);
    return s;
  }

  private synchronized <EC> void addStage(String name, Stage<EC> s) {
    Object prev = stages.put(name, s);
    Assert.assertNull(prev);
    stageNames = stages.keySet().toArray(new String[stages.size()]);
    Arrays.sort(stageNames);
  }

  @Override
  public synchronized void startAll(ConfigurationContext context, List<PostInit> toInit, String...exclusion) {
    for (PostInit mgr : toInit) {
      mgr.initializeContext(context);

    }
    Arrays.sort(exclusion);
    for (Stage<?> s : stages.values()) {
      if (Arrays.binarySearch(exclusion, s.getName()) < 0) {
        s.start(context);
      }
    }
    started = true;
  }

  @Override
  public void stopAll() {
    for (Stage<?> s : stages.values()) {
      s.stop();
    }
    stages.clear();
    this.classVerifications.clear();
    started = false;
  }

  @SuppressWarnings("unchecked")
  @Override
  public  <EC> Stage<EC> getStage(String name, Class<EC> verification) {
    Stage<EC> stage = (Stage<EC>) stages.get(name);
    if (stage != null) {
      Assert.assertTrue(verification.isAssignableFrom(classVerifications.get(name)));
    }
    return stage;
  }

  @Override
  public synchronized Stats[] getStats() {
    final String[] names = stageNames;
    final Stats[] stats = new Stats[names.length];

    for (int i = 0; i < names.length; i++) {
      Map<String, ?> data = stages.get(names[i]).getState();
      stats[i] = new Stats() {
        @Override
        public String getDetails() {
          MapListPrettyPrint pp = new MapListPrettyPrint();
          pp.println(data);
          return pp.toString();
        }

        @Override
        public void logDetails(Logger statsLogger) {
          statsLogger.info(getDetails());
        }
      };
    }
    return stats;
  }

  @Override
  public Map<String, ?> getStateMap() {
    Map<String,Object> map = new LinkedHashMap<>();
    map.put("className", this.getClass().getName());
    map.put("monitor", MONITOR);
    List<Object> list = new ArrayList<>(stages.size());
    for (Stage<?> stage : stages.values()) {
      if (stage.isStarted()) {
        list.add(stage.getState());
      }
    }
    map.put("stages", list);
    return map;
  }

  @Override
  public int getDefaultStageMaximumCapacity() {
    return DEFAULT_MAX_STAGE_CAPACITY;
  }
}
