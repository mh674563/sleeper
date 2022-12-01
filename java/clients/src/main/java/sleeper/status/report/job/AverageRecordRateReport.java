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
package sleeper.status.report.job;

import sleeper.core.record.process.AverageRecordRate;

import java.io.PrintStream;

import static sleeper.status.report.job.StandardProcessRunReporter.formatDecimal;

public class AverageRecordRateReport {

    private AverageRecordRateReport() {
    }

    public static void printf(String format, AverageRecordRate average, PrintStream out) {
        if (average.getJobCount() < 1) {
            return;
        }
        String rateString = String.format("%s read/s, %s write/s",
                formatDecimal(average.getRecordsReadPerSecond()),
                formatDecimal(average.getRecordsWrittenPerSecond()));
        out.printf(format, rateString);
    }
}
