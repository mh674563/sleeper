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

package sleeper.ingest;

import org.apache.hadoop.conf.Configuration;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.core.schema.Schema;
import sleeper.statestore.StateStore;

public class IngestProperties {
    private ObjectFactory objectFactory;
    private String localDir;
    private long maxRecordsToWriteLocally;
    private long maxInMemoryBatchSize;
    private int rowGroupSize;
    private int pageSize;
    private String compressionCodec;
    private StateStore stateStore;
    private Schema schema;
    private String fs;
    private String bucketName;
    private String iteratorClassName;
    private String iteratorConfig;
    private int ingestPartitionRefreshFrequencyInSecond;
    private Configuration hadoopConfiguration;

    private IngestProperties(Builder builder) {
        objectFactory = builder.objectFactory;
        localDir = builder.localDir;
        maxRecordsToWriteLocally = builder.maxRecordsToWriteLocally;
        maxInMemoryBatchSize = builder.maxInMemoryBatchSize;
        rowGroupSize = builder.rowGroupSize;
        pageSize = builder.pageSize;
        compressionCodec = builder.compressionCodec;
        stateStore = builder.stateStore;
        schema = builder.schema;
        fs = builder.fs;
        bucketName = builder.bucketName;
        iteratorClassName = builder.iteratorClassName;
        iteratorConfig = builder.iteratorConfig;
        ingestPartitionRefreshFrequencyInSecond = builder.ingestPartitionRefreshFrequencyInSecond;
        hadoopConfiguration = builder.hadoopConfiguration;
    }

    public ObjectFactory getObjectFactory() {
        return objectFactory;
    }

    public String getLocalDir() {
        return localDir;
    }

    public long getMaxRecordsToWriteLocally() {
        return maxRecordsToWriteLocally;
    }

    public long getMaxInMemoryBatchSize() {
        return maxInMemoryBatchSize;
    }

    public int getRowGroupSize() {
        return rowGroupSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public String getCompressionCodec() {
        return compressionCodec;
    }

    public StateStore getStateStore() {
        return stateStore;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getFilePrefix() {
        return fs;
    }

    public String getBucketName() {
        return null == bucketName ? "" : bucketName;
    }

    public String getIteratorClassName() {
        return iteratorClassName;
    }

    public String getIteratorConfig() {
        return iteratorConfig;
    }

    public int getIngestPartitionRefreshFrequencyInSecond() {
        return ingestPartitionRefreshFrequencyInSecond;
    }

    public Configuration getHadoopConfiguration() {
        return hadoopConfiguration;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return IngestProperties.builder()
                .objectFactory(objectFactory)
                .localDir(localDir)
                .maxRecordsToWriteLocally(maxRecordsToWriteLocally)
                .maxInMemoryBatchSize(maxInMemoryBatchSize)
                .rowGroupSize(rowGroupSize)
                .pageSize(pageSize)
                .compressionCodec(compressionCodec)
                .stateStore(stateStore)
                .schema(schema)
                .filePathPrefix(fs)
                .bucketName(bucketName)
                .iteratorClassName(iteratorClassName)
                .iteratorConfig(iteratorConfig)
                .ingestPartitionRefreshFrequencyInSecond(ingestPartitionRefreshFrequencyInSecond)
                .hadoopConfiguration(hadoopConfiguration);
    }

    public static final class Builder {
        private ObjectFactory objectFactory;
        private String localDir;
        private long maxRecordsToWriteLocally;
        private long maxInMemoryBatchSize;
        private int rowGroupSize;
        private int pageSize;
        private String compressionCodec;
        private StateStore stateStore;
        private Schema schema;
        private String fs;
        private String bucketName;
        private String iteratorClassName;
        private String iteratorConfig;
        private int ingestPartitionRefreshFrequencyInSecond;
        private Configuration hadoopConfiguration;

        private Builder() {
        }

        public Builder objectFactory(ObjectFactory objectFactory) {
            this.objectFactory = objectFactory;
            return this;
        }

        public Builder localDir(String localDir) {
            this.localDir = localDir;
            return this;
        }

        public Builder maxRecordsToWriteLocally(long maxRecordsToWriteLocally) {
            this.maxRecordsToWriteLocally = maxRecordsToWriteLocally;
            return this;
        }

        public Builder maxInMemoryBatchSize(long maxInMemoryBatchSize) {
            this.maxInMemoryBatchSize = maxInMemoryBatchSize;
            return this;
        }

        public Builder rowGroupSize(int rowGroupSize) {
            this.rowGroupSize = rowGroupSize;
            return this;
        }

        public Builder pageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        public Builder compressionCodec(String compressionCodec) {
            this.compressionCodec = compressionCodec;
            return this;
        }

        public Builder stateStore(StateStore stateStore) {
            this.stateStore = stateStore;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder filePathPrefix(String fs) {
            this.fs = fs;
            return this;
        }

        public Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder iteratorClassName(String iteratorClassName) {
            this.iteratorClassName = iteratorClassName;
            return this;
        }

        public Builder iteratorConfig(String iteratorConfig) {
            this.iteratorConfig = iteratorConfig;
            return this;
        }

        public Builder ingestPartitionRefreshFrequencyInSecond(int ingestPartitionRefreshFrequencyInSecond) {
            this.ingestPartitionRefreshFrequencyInSecond = ingestPartitionRefreshFrequencyInSecond;
            return this;
        }

        public Builder hadoopConfiguration(Configuration hadoopConfiguration) {
            this.hadoopConfiguration = hadoopConfiguration;
            return this;
        }

        public IngestProperties build() {
            return new IngestProperties(this);
        }
    }
}