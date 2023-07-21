/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.systemtest.ingest;

import sleeper.systemtest.SystemTestProperties;

import static sleeper.systemtest.SystemTestProperty.MAX_ENTRIES_RANDOM_LIST;
import static sleeper.systemtest.SystemTestProperty.MAX_ENTRIES_RANDOM_MAP;
import static sleeper.systemtest.SystemTestProperty.MAX_RANDOM_INT;
import static sleeper.systemtest.SystemTestProperty.MAX_RANDOM_LONG;
import static sleeper.systemtest.SystemTestProperty.MIN_RANDOM_INT;
import static sleeper.systemtest.SystemTestProperty.MIN_RANDOM_LONG;
import static sleeper.systemtest.SystemTestProperty.RANDOM_BYTE_ARRAY_LENGTH;
import static sleeper.systemtest.SystemTestProperty.RANDOM_STRING_LENGTH;

/**
 * Contains parameters to control the random data generated by a <code>RandomRecordSupplier</code>.
 */
public class RandomRecordSupplierConfig {
    private final int minRandomInt;
    private final int maxRandomInt;
    private final long minRandomLong;
    private final long maxRandomLong;
    private final int randomStringLength;
    private final int randomByteArrayLength;
    private final int maxEntriesInRandomMap;
    private final int maxEntriesInRandomList;

    public RandomRecordSupplierConfig(SystemTestProperties systemTestProperties) {
        this(systemTestProperties.getInt(MIN_RANDOM_INT),
                systemTestProperties.getInt(MAX_RANDOM_INT),
                systemTestProperties.getLong(MIN_RANDOM_LONG),
                systemTestProperties.getLong(MAX_RANDOM_LONG),
                systemTestProperties.getInt(RANDOM_STRING_LENGTH),
                systemTestProperties.getInt(RANDOM_BYTE_ARRAY_LENGTH),
                systemTestProperties.getInt(MAX_ENTRIES_RANDOM_MAP),
                systemTestProperties.getInt(MAX_ENTRIES_RANDOM_LIST));
    }

    public RandomRecordSupplierConfig(int minRandomInt,
                                      int maxRandomInt,
                                      long minRandomLong,
                                      long maxRandomLong,
                                      int randomStringLength,
                                      int randomByteArrayLength,
                                      int maxEntriesInRandomMap,
                                      int maxEntriesInRandomList) {
        this.minRandomInt = minRandomInt;
        this.maxRandomInt = maxRandomInt;
        this.minRandomLong = minRandomLong;
        this.maxRandomLong = maxRandomLong;
        this.randomStringLength = randomStringLength;
        this.randomByteArrayLength = randomByteArrayLength;
        this.maxEntriesInRandomMap = maxEntriesInRandomMap;
        this.maxEntriesInRandomList = maxEntriesInRandomList;
    }

    public int getMinRandomInt() {
        return minRandomInt;
    }

    public int getMaxRandomInt() {
        return maxRandomInt;
    }

    public long getMinRandomLong() {
        return minRandomLong;
    }

    public long getMaxRandomLong() {
        return maxRandomLong;
    }

    public int getRandomStringLength() {
        return randomStringLength;
    }

    public int getRandomByteArrayLength() {
        return randomByteArrayLength;
    }

    public int getMaxEntriesInRandomMap() {
        return maxEntriesInRandomMap;
    }

    public int getMaxEntriesInRandomList() {
        return maxEntriesInRandomList;
    }
}