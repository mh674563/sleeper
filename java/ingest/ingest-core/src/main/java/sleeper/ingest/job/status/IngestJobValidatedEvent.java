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

package sleeper.ingest.job.status;

import sleeper.ingest.job.IngestJob;

import java.time.Instant;
import java.util.Objects;

public class IngestJobValidatedEvent {
    private final String taskId;
    private final IngestJob job;
    private final Instant validationTime;
    private final String reason;

    private IngestJobValidatedEvent(String taskId, IngestJob job, Instant validationTime, String reason) {
        this.taskId = taskId;
        this.job = job;
        this.validationTime = validationTime;
        this.reason = reason;
    }

    public static IngestJobValidatedEvent ingestJobAccepted(String taskId, IngestJob job, Instant validationTime) {
        return new IngestJobValidatedEvent(taskId, job, validationTime, null);
    }

    public static IngestJobValidatedEvent ingestJobRejected(String taskId, IngestJob job, Instant validationTime, String reason) {
        return new IngestJobValidatedEvent(taskId, job, validationTime, reason);
    }

    public String getTaskId() {
        return taskId;
    }

    public IngestJob getJob() {
        return job;
    }

    public Instant getValidationTime() {
        return validationTime;
    }

    public boolean isAccepted() {
        return reason == null;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IngestJobValidatedEvent that = (IngestJobValidatedEvent) o;
        return Objects.equals(taskId, that.taskId)
                && Objects.equals(job, that.job)
                && Objects.equals(validationTime, that.validationTime)
                && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskId, job, validationTime, reason);
    }

    @Override
    public String toString() {
        return "IngestJobValidatedEvent{" +
                "taskId='" + taskId + '\'' +
                ", job=" + job +
                ", validationTime=" + validationTime +
                ", reason='" + reason + '\'' +
                '}';
    }
}