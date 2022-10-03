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
package sleeper.compaction.status.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.After;
import org.junit.Before;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.compaction.job.CompactionJobRecordsProcessed;
import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.compaction.job.CompactionJobSummary;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobFinishedStatus;
import sleeper.compaction.job.status.CompactionJobStartedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore;
import sleeper.compaction.status.job.DynamoDBCompactionJobStatusStoreCreator;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionsBuilder;
import sleeper.core.partition.PartitionsFromSplitPoints;
import sleeper.core.schema.Schema;
import sleeper.statestore.FileInfoFactory;

import java.time.Instant;
import java.time.Period;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static sleeper.compaction.status.job.DynamoDBCompactionJobStatusStore.jobStatusTableName;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createInstanceProperties;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createSchema;
import static sleeper.compaction.status.testutils.CompactionStatusStoreTestUtils.createTableProperties;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class DynamoDBCompactionJobStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_UPDATE_TIMES = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("createdStatus.updateTime", "startedStatus.updateTime", "finishedStatus.updateTime", "expiryDate").build();

    private final InstanceProperties instanceProperties = createInstanceProperties();
    private final String jobStatusTableName = jobStatusTableName(instanceProperties.get(ID));
    private final Schema schema = createSchema();
    private final TableProperties tableProperties = createTableProperties(schema, instanceProperties);

    protected final String tableName = tableProperties.get(TABLE_NAME);
    protected final CompactionJobFactory jobFactory = new CompactionJobFactory(instanceProperties, tableProperties);
    protected final CompactionJobStatusStore store = DynamoDBCompactionJobStatusStore.from(dynamoDBClient, instanceProperties);

    @Before
    public void setUp() {
        DynamoDBCompactionJobStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(jobStatusTableName);
    }

    protected Partition singlePartition() {
        return new PartitionsFromSplitPoints(schema, Collections.emptyList()).construct().get(0);
    }

    protected FileInfoFactory fileFactory(Partition singlePartition) {
        return fileFactory(Collections.singletonList(singlePartition));
    }

    protected FileInfoFactory fileFactoryWithPartitions(Consumer<PartitionsBuilder> partitionConfig) {
        PartitionsBuilder builder = new PartitionsBuilder(schema);
        partitionConfig.accept(builder);
        return fileFactory(builder.buildList());
    }

    private FileInfoFactory fileFactory(List<Partition> partitions) {
        return new FileInfoFactory(schema, partitions, Instant.now());
    }

    protected CompactionJobFactory jobFactoryForTable(String tableName) {
        TableProperties tableProperties = createTableProperties(schema, instanceProperties);
        tableProperties.set(TABLE_NAME, tableName);
        return new CompactionJobFactory(instanceProperties, tableProperties);
    }

    protected static Instant ignoredUpdateTime() {
        return Instant.now();
    }

    protected static Instant defaultStartTime() {
        return Instant.parse("2022-09-23T10:51:00.001Z");
    }

    protected static CompactionJobSummary defaultSummary() {
        return new CompactionJobSummary(
                new CompactionJobRecordsProcessed(200L, 100L),
                defaultStartTime(), Instant.parse("2022-09-23T10:52:00.001Z"));
    }

    protected static CompactionJobStatus startedStatusWithDefaults(CompactionJob job) {
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(
                        job, ignoredUpdateTime()))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTime(
                        ignoredUpdateTime(), defaultStartTime()))
                .build();
    }

    protected static CompactionJobStatus finishedStatusWithDefaults(CompactionJob job) {
        return CompactionJobStatus.builder().jobId(job.getId())
                .createdStatus(CompactionJobCreatedStatus.from(
                        job, ignoredUpdateTime()))
                .startedStatus(CompactionJobStartedStatus.updateAndStartTime(
                        ignoredUpdateTime(), defaultStartTime()))
                .finishedStatus(CompactionJobFinishedStatus.updateTimeAndSummary(
                        ignoredUpdateTime(), defaultSummary()))
                .build();
    }


    protected List<CompactionJobStatus> getAllJobStatuses() {
        Instant epochStart = Instant.ofEpochMilli(0);
        Instant farFuture = epochStart.plus(Period.ofDays(999999999));
        return store.getJobsInTimePeriod(tableName, epochStart, farFuture);
    }
}