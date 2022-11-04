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

package sleeper.status.report.compactionjob;

import org.junit.Test;
import sleeper.compaction.job.status.CompactionJobStatus;
import sleeper.status.report.compactionjob.CompactionJobStatusReporter.QueryType;

import java.util.Collections;
import java.util.List;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ClientTestUtils.example;

public class StatusReporterUnfinishedQueryTest extends StatusReporterTestBase {

    @Test
    public void shouldReportCompactionJobStatusForUnfinishedStandardAndSplittingCompactions() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = mixedUnfinishedJobStatuses();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(replaceStandardJobIds(statusList, example("reports/compactionjobstatus/standard/unfinished/mixedUnfinishedJobs.txt")));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(replaceBracketedJobIds(statusList, example("reports/compactionjobstatus/json/mixedUnfinishedJobs.json")));
    }

    @Test
    public void shouldReportNoCompactionJobStatusIfNoJobsUnfinished() throws Exception {
        // Given
        List<CompactionJobStatus> statusList = Collections.emptyList();

        // When / Then
        assertThat(verboseReportString(StandardCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/standard/unfinished/noJobs.txt"));
        assertThatJson(verboseReportString(JsonCompactionJobStatusReporter::new, statusList, QueryType.UNFINISHED))
                .isEqualTo(example("reports/compactionjobstatus/json/noJobs.json"));
    }
}