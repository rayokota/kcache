/*
 * Copyright 2014-2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kcache.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownableThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(ShutdownableThread.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    public ShutdownableThread(String name) {
        super(name);
        this.setDaemon(false);
    }

    public void shutdown() throws InterruptedException {
        initiateShutdown();
        awaitShutdown();
    }

    protected void initiateShutdown() {
        isRunning.set(false);
    }

    protected void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
        log.info("Shutdown completed");
    }

    /**
     * This method is repeatedly invoked until the thread shuts down or this method throws an exception
     */
    protected abstract void doWork();

    @Override
    public void run() {
        log.info("Starting");
        try {
            while (isRunning()) {
                doWork();
            }
        } catch (Error | RuntimeException e) {
            log.error("Thread {} exiting with uncaught exception: ", getName(), e);
            throw e;
        } finally {
            shutdownLatch.countDown();
        }
        log.info("Stopped");
    }

    public boolean isRunning() {
        return isRunning.get();
    }
}
