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
package sleeper.compaction.status.store.testutils;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.After;
import org.junit.Before;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStore;
import sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStoreCreator;
import sleeper.compaction.task.CompactionTaskFinishedStatus;
import sleeper.compaction.task.CompactionTaskStatus;
import sleeper.compaction.task.CompactionTaskStatusStore;
import sleeper.compaction.task.CompactionTaskType;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.core.record.process.RecordsProcessed;
import sleeper.core.record.process.RecordsProcessedSummary;
import sleeper.dynamodb.tools.DynamoDBTestBase;

import java.time.Instant;
import java.util.UUID;

import static sleeper.compaction.status.store.task.DynamoDBCompactionTaskStatusStore.taskStatusTableName;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;

public class DynamoDBCompactionTaskStatusStoreTestBase extends DynamoDBTestBase {

    protected static final RecursiveComparisonConfiguration IGNORE_EXPIRY_DATE = RecursiveComparisonConfiguration.builder()
            .withIgnoredFields("expiryDate").build();
    private final InstanceProperties instanceProperties = CompactionStatusStoreTestUtils.createInstanceProperties();
    private final String taskStatusTableName = taskStatusTableName(instanceProperties.get(ID));
    protected final CompactionTaskStatusStore store = DynamoDBCompactionTaskStatusStore.from(dynamoDBClient, instanceProperties);

    @Before
    public void setUp() {
        DynamoDBCompactionTaskStatusStoreCreator.create(instanceProperties, dynamoDBClient);
    }

    @After
    public void tearDown() {
        dynamoDBClient.deleteTable(taskStatusTableName);
    }

    private static Instant defaultJobStartTime() {
        return Instant.parse("2022-09-22T14:00:04.000Z");
    }

    private static Instant defaultJobFinishTime() {
        return Instant.parse("2022-09-22T14:00:14.000Z");
    }

    private static Instant defaultTaskStartTime() {
        return Instant.parse("2022-09-22T12:30:00.000Z");
    }

    private static Instant defaultTaskFinishTime() {
        return Instant.parse("2022-09-22T16:30:00.000Z");
    }

    private static Instant taskFinishTimeWithDurationInSecondsNotAWholeNumber() {
        return Instant.parse("2022-09-22T16:30:00.500Z");
    }

    private static RecordsProcessedSummary defaultJobSummary() {
        return new RecordsProcessedSummary(
                new RecordsProcessed(4800L, 2400L),
                defaultJobStartTime(), defaultJobFinishTime());
    }

    protected static CompactionTaskStatus startedTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().build();
    }

    protected static CompactionTaskStatus splittingTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().type(CompactionTaskType.SPLITTING).build();
    }

    protected static CompactionTaskStatus.Builder startedTaskWithDefaultsBuilder() {
        return CompactionTaskStatus.builder().taskId(UUID.randomUUID().toString()).startTime(defaultTaskStartTime());
    }

    protected static CompactionTaskStatus finishedTaskWithDefaults() {
        return startedTaskWithDefaultsBuilder().finished(
                CompactionTaskFinishedStatus.builder()
                        .addJobSummary(defaultJobSummary()),
                defaultTaskFinishTime().toEpochMilli()).build();
    }

    protected static CompactionTaskStatus finishedTaskWithDefaultsAndDurationInSecondsNotAWholeNumber() {
        return startedTaskWithDefaultsBuilder().finished(
                CompactionTaskFinishedStatus.builder()
                        .addJobSummary(defaultJobSummary()),
                taskFinishTimeWithDurationInSecondsNotAWholeNumber().toEpochMilli()).build();
    }

    protected static CompactionTaskStatus taskWithStartTime(Instant startTime) {
        return taskBuilder().startTime(startTime).build();
    }

    protected static CompactionTaskStatus taskWithStartAndFinishTime(Instant startTime, Instant finishTime) {
        return buildWithStartAndFinishTime(taskBuilder(), startTime, finishTime);
    }

    protected static CompactionTaskStatus splittingTaskWithStartTime(Instant startTime) {
        return taskBuilder().type(CompactionTaskType.SPLITTING).startTime(startTime).build();
    }

    protected static CompactionTaskStatus splittingTaskWithStartAndFinishTime(Instant startTime, Instant finishTime) {
        return buildWithStartAndFinishTime(taskBuilder().type(CompactionTaskType.SPLITTING), startTime, finishTime);
    }

    private static CompactionTaskStatus.Builder taskBuilder() {
        return CompactionTaskStatus.builder().taskId(UUID.randomUUID().toString());
    }

    private static CompactionTaskStatus buildWithStartAndFinishTime(
            CompactionTaskStatus.Builder builder, Instant startTime, Instant finishTime) {
        return builder.startTime(startTime)
                .finished(CompactionTaskFinishedStatus.builder()
                        .addJobSummary(new RecordsProcessedSummary(
                                new RecordsProcessed(200, 100),
                                startTime, finishTime
                        )), finishTime)
                .build();
    }

}