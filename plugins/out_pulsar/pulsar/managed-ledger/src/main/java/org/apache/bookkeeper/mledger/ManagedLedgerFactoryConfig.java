/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger;

import lombok.Data;

/**
 * Configuration for a {@link ManagedLedgerFactory}.
 */
@Data
public class ManagedLedgerFactoryConfig {
    private static final long MB = 1024 * 1024;

    private long maxCacheSize = 128 * MB;

    /**
     * The cache eviction watermark is the percentage of the cache size to reach when removing entries from the cache.
     */
    private double cacheEvictionWatermark = 0.90;

    private int numManagedLedgerWorkerThreads = Runtime.getRuntime().availableProcessors();
    private int numManagedLedgerSchedulerThreads = Runtime.getRuntime().availableProcessors();

    public long getMaxCacheSize() {
        return maxCacheSize;
    }
}
