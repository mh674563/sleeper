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
package sleeper.ingest.impl.partitionfilewriter;

import sleeper.core.record.Record;
import sleeper.core.statestore.FileInfo;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * This interface describes classes which generate Sleeper partition files. Each implementing class allows data to be
 * appended, in sort order, to a single partition file. The class is responsible for transferring the partition file to
 * any remote system, possibly asynchronously.
 */
public interface PartitionFileWriter {
    /**
     * Append a {@link Record} to the partition file. This method must always be called with records increasing in sort
     * order.
     *
     * @param record The record to append
     * @throws IOException -
     */
    void append(Record record) throws IOException;

    /**
     * Close the file, possibly asynchronously. When the returned future completes, the partition file should be in its
     * final storage and any intermediate data should be cleared.
     *
     * @return Details about the new partition file
     * @throws IOException -
     */
    CompletableFuture<FileInfo> close() throws IOException;

    /**
     * Cancel this write operation and clear all intermediate data.
     */
    void abort();
}
