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
package com.tc.net.core.event;

/**
 * Interface for receiving connection events. These event handler will more than likely be called on the main
 * communications thread. It is unwise to do "heavy" operations in these handlers. In particular one should not be
 * calling back into the comms layer (for example to create a new connection or something)
 * <p>
 * Exceptions that occur in event handlers will caught and logged. An exception in any one event listener will not
 * prevent other listeners from also receiving the event
 * 
 * @author teck
 */
public interface TCConnectionEventListener {

  /**
   * Connect event will be called once per connection connect
   */
  public void connectEvent(TCConnectionEvent event);

  /**
   * Connection closed event will only be called once and will only be called if connection was actually ever connected.
   * If a non-connected channel is closed you will not receice this event
   */
  public void closeEvent(TCConnectionEvent event);

  /**
   * Error event will be called any time an exception occurs as part of doing an IO operation (read, write, connect,
   * accept) on a given connection
   */
  public void errorEvent(TCConnectionErrorEvent errorEvent);

  /**
   * EOF event is thrown when a clean EOF is read from the network. If a network exception prematurely causes a
   * connection to end, you won't see an EOF event
   */
  public void endOfFileEvent(TCConnectionEvent event);
}
