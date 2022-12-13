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
package sleeper.compaction.status.store.job;

import org.junit.Test;
import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.status.CompactionJobCreatedStatus;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.compaction.status.store.testutils.DynamoDBCompactionJobStatusStoreTestBase;
import sleeper.core.partition.Partition;
import sleeper.core.record.process.status.ProcessRun;
import sleeper.core.record.process.status.ProcessStartedStatus;
import sleeper.statestore.FileInfoFactory;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryCompactionJobStatusByTaskIdIT extends DynamoDBCompactionJobStatusStoreTestBase {

    @Test
    public void shouldReturnCompactionJobsByTaskId() {
        // Given
        String searchingTaskId = "test-task";
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job1 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());
        CompactionJob job2 = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file2", 456L, "d", "f")),
                partition.getId());

        // When
        store.jobCreated(job1);
        store.jobCreated(job2);
        store.jobStarted(job1, defaultStartTime(), searchingTaskId);
        store.jobStarted(job2, defaultStartTime(), "another-task");

        // Then
        assertThat(store.getJobsByTaskId(tableName, searchingTaskId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.builder().jobId(job1.getId())
                        .createdStatus(CompactionJobCreatedStatus.from(job1, ignoredUpdateTime()))
                        .singleJobRun(ProcessRun.started(searchingTaskId,
                                ProcessStartedStatus.updateAndStartTime(ignoredUpdateTime(), defaultStartTime())))
                        .build());
    }

    @Test
    public void shouldReturnCompactionJobByTaskIdInOneRun() {
        // Given
        String taskId1 = "task-id-1";
        String searchingTaskId = "test-task";
        String taskId3 = "task-id-3";
        Partition partition = singlePartition();
        FileInfoFactory fileFactory = fileFactory(partition);
        CompactionJob job = jobFactory.createCompactionJob(
                Collections.singletonList(fileFactory.leafFile("file1", 123L, "a", "c")),
                partition.getId());

        // When
        store.jobCreated(job);
        store.jobStarted(job, defaultStartTime(), taskId1);
        store.jobStarted(job, defaultStartTime(), searchingTaskId);
        store.jobStarted(job, defaultStartTime(), taskId3);

        // Then
        assertThat(store.getJobsByTaskId(tableName, searchingTaskId))
                .usingRecursiveFieldByFieldElementComparator(IGNORE_UPDATE_TIMES)
                .containsExactly(CompactionJobStatus.builder().jobId(job.getId())
                        .createdStatus(CompactionJobCreatedStatus.from(job, ignoredUpdateTime()))
                        .jobRunsLatestFirst(Arrays.asList(
                                ProcessRun.started(taskId3,
                                        ProcessStartedStatus.updateAndStartTime(ignoredUpdateTime(), defaultStartTime())),
                                ProcessRun.started(searchingTaskId,
                                        ProcessStartedStatus.updateAndStartTime(ignoredUpdateTime(), defaultStartTime())),
                                ProcessRun.started(taskId1,
                                        ProcessStartedStatus.updateAndStartTime(ignoredUpdateTime(), defaultStartTime()))
                        )).build());
    }

    @Test
    public void shouldReturnNoCompactionJobsByTaskId() {
        // When / Then
        assertThat(store.getJobsByTaskId(tableName, "not-present")).isNullOrEmpty();
    }
}