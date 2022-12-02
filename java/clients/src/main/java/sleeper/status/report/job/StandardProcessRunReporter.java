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

import sleeper.core.record.process.status.ProcessRun;
import sleeper.status.report.table.TableField;
import sleeper.status.report.table.TableRow;
import sleeper.status.report.table.TableWriterFactory;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static sleeper.ClientUtils.countWithCommas;
import static sleeper.ClientUtils.decimalWithCommas;

public class StandardProcessRunReporter {
    private final TableField taskIdField;
    private final TableField startTimeField;
    private final TableField finishTimeField;
    private final TableField durationField;
    private final TableField linesReadField;
    private final TableField linesWrittenField;
    private final TableField readRateField;
    private final TableField writeRateField;
    private final PrintStream out;
    public static final String STATE_IN_PROGRESS = "IN PROGRESS";
    public static final String STATE_FINISHED = "FINISHED";

    public StandardProcessRunReporter(PrintStream out, TableWriterFactory.Builder tableBuilder) {
        this(builder().out(out)
                .taskIdField(tableBuilder.addField("TASK_ID"))
                .startTimeField(tableBuilder.addField("START_TIME"))
                .finishTimeField(tableBuilder.addField("FINISH_TIME"))
                .durationField(tableBuilder.addNumericField("DURATION (s)"))
                .linesReadField(tableBuilder.addNumericField("LINES_READ"))
                .linesWrittenField(tableBuilder.addNumericField("LINES_WRITTEN"))
                .readRateField(tableBuilder.addNumericField("READ_RATE (s)"))
                .writeRateField(tableBuilder.addNumericField("WRITE_RATE (s)")));
    }

    private StandardProcessRunReporter(Builder builder) {
        taskIdField = builder.taskIdField;
        startTimeField = builder.startTimeField;
        finishTimeField = builder.finishTimeField;
        durationField = builder.durationField;
        linesReadField = builder.linesReadField;
        linesWrittenField = builder.linesWrittenField;
        readRateField = builder.readRateField;
        writeRateField = builder.writeRateField;
        out = builder.out;
    }

    public static Builder builder() {
        return new Builder();
    }

    public void writeRunFields(ProcessRun run, TableRow.Builder builder) {
        builder.value(taskIdField, run.getTaskId())
                .value(startTimeField, run.getStartTime())
                .value(finishTimeField, run.getFinishTime())
                .value(durationField, getDurationInSeconds(run))
                .value(linesReadField, getLinesRead(run))
                .value(linesWrittenField, getLinesWritten(run))
                .value(readRateField, getRecordsReadPerSecond(run))
                .value(writeRateField, getRecordsWrittenPerSecond(run));
    }

    public void printProcessJobRun(ProcessRun run) {
        out.println();
        out.printf("Run on task %s%n", run.getTaskId());
        out.printf("Start Time: %s%n", run.getStartTime());
        out.printf("Start Update Time: %s%n", run.getStartUpdateTime());
        if (run.isFinished()) {
            out.printf("Finish Time: %s%n", run.getFinishTime());
            out.printf("Finish Update Time: %s%n", run.getFinishUpdateTime());
            out.printf("Duration: %ss%n", getDurationInSeconds(run));
            out.printf("Lines Read: %s%n", getLinesRead(run));
            out.printf("Lines Written: %s%n", getLinesWritten(run));
            out.printf("Read Rate (reads per second): %s%n", getRecordsReadPerSecond(run));
            out.printf("Write Rate (writes per second): %s%n", getRecordsWrittenPerSecond(run));
        } else {
            out.println("Not finished");
        }
    }

    public List<TableField> getFinishedFields() {
        return Arrays.asList(finishTimeField, durationField, linesReadField, linesWrittenField, readRateField, writeRateField);
    }

    public static String getState(ProcessRun run) {
        if (run.isFinished()) {
            return STATE_FINISHED;
        }
        return STATE_IN_PROGRESS;
    }

    public static String getDurationInSeconds(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getDurationInSeconds()));
    }

    public static String getLinesRead(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> countWithCommas(summary.getLinesRead()));
    }

    public static String getLinesWritten(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> countWithCommas(summary.getLinesWritten()));
    }

    public static String getRecordsReadPerSecond(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getRecordsReadPerSecond()));
    }

    public static String getRecordsWrittenPerSecond(ProcessRun run) {
        return getOrNull(run.getFinishedSummary(), summary -> formatDecimal(summary.getRecordsWrittenPerSecond()));
    }

    public static String formatDecimal(double value) {
        return decimalWithCommas("%.2f", value);
    }

    public static <I, O> O getOrNull(I object, Function<I, O> getter) {
        if (object == null) {
            return null;
        }
        return getter.apply(object);
    }

    public static final class Builder {
        private TableField taskIdField;
        private TableField startTimeField;
        private TableField finishTimeField;
        private TableField durationField;
        private TableField linesReadField;
        private TableField linesWrittenField;
        private TableField readRateField;
        private TableField writeRateField;
        private PrintStream out;

        private Builder() {
        }

        public Builder taskIdField(TableField taskIdField) {
            this.taskIdField = taskIdField;
            return this;
        }

        public Builder startTimeField(TableField startTimeField) {
            this.startTimeField = startTimeField;
            return this;
        }

        public Builder finishTimeField(TableField finishTimeField) {
            this.finishTimeField = finishTimeField;
            return this;
        }

        public Builder durationField(TableField durationField) {
            this.durationField = durationField;
            return this;
        }

        public Builder linesReadField(TableField linesReadField) {
            this.linesReadField = linesReadField;
            return this;
        }

        public Builder linesWrittenField(TableField linesWrittenField) {
            this.linesWrittenField = linesWrittenField;
            return this;
        }

        public Builder readRateField(TableField readRateField) {
            this.readRateField = readRateField;
            return this;
        }

        public Builder writeRateField(TableField writeRateField) {
            this.writeRateField = writeRateField;
            return this;
        }

        public Builder out(PrintStream out) {
            this.out = out;
            return this;
        }

        public StandardProcessRunReporter build() {
            return new StandardProcessRunReporter(this);
        }
    }
}
