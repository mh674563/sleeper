/*
 * Copyright 2022 Crown Copyright
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

import sleeper.core.partition.Partition;
import sleeper.ingest.impl.ParquetConfiguration;

import java.io.IOException;
import java.util.Objects;

public class DirectPartitionFileWriterFactory implements PartitionFileWriterFactory {

    private final ParquetConfiguration parquetConfiguration;
    private final String fileSystem;

    public DirectPartitionFileWriterFactory(ParquetConfiguration parquetConfiguration, String fileSystem) {
        this.parquetConfiguration = Objects.requireNonNull(parquetConfiguration, "parquetWriterConfiguration must not be null");
        this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem must not be null");
    }

    @Override
    public PartitionFileWriter createPartitionFileWriter(Partition partition) {
        try {
            return new DirectPartitionFileWriter(
                    partition,
                    parquetConfiguration,
                    fileSystem);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}