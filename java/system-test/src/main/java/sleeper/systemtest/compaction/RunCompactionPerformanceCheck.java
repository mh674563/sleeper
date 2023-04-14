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

package sleeper.systemtest.compaction;

public class RunCompactionPerformanceCheck {
    private final int expectedNumOfJobs;
    private final int expectedNumOfRecordsInRoot;
    private final double previousReadPerformance;
    private final double previousWritePerformance;
    private final CompactionPerformanceChecker checker;

    private RunCompactionPerformanceCheck(Builder builder) {
        expectedNumOfJobs = builder.expectedNumOfJobs;
        expectedNumOfRecordsInRoot = builder.expectedNumOfRecordsInRoot;
        previousReadPerformance = builder.previousReadPerformance;
        previousWritePerformance = builder.previousWritePerformance;
        checker = builder.checker;
    }


    public void run() throws CompactionPerformanceChecker.CheckFailedException {
        checker.check(expectedNumOfJobs, expectedNumOfRecordsInRoot,
                previousReadPerformance, previousWritePerformance);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int expectedNumOfJobs;
        private int expectedNumOfRecordsInRoot;
        private double previousReadPerformance = 0;
        private double previousWritePerformance = 0;
        private CompactionPerformanceChecker checker;

        private Builder() {
        }

        public Builder expectedNumOfJobs(int expectedNumOfJobs) {
            this.expectedNumOfJobs = expectedNumOfJobs;
            return this;
        }

        public Builder expectedNumOfRecordsInRoot(int expectedNumOfRecordsInRoot) {
            this.expectedNumOfRecordsInRoot = expectedNumOfRecordsInRoot;
            return this;
        }

        public Builder checker(CompactionPerformanceChecker checker) {
            this.checker = checker;
            return this;
        }

        public RunCompactionPerformanceCheck build() {
            return new RunCompactionPerformanceCheck(this);
        }
    }
}
