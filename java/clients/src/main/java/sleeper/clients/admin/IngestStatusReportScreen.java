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

package sleeper.clients.admin;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.console.ConsoleHelper;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.MenuOption;
import sleeper.status.report.IngestJobStatusReport;
import sleeper.status.report.IngestTaskStatusReport;
import sleeper.status.report.ingest.job.IngestJobStatusReportArguments;
import sleeper.status.report.ingest.job.StandardIngestJobStatusReporter;
import sleeper.status.report.ingest.task.IngestTaskQuery;
import sleeper.status.report.ingest.task.StandardIngestTaskStatusReporter;
import sleeper.status.report.job.query.JobQuery;

import java.util.Optional;

import static sleeper.clients.admin.AdminCommonPrompts.confirmReturnToMainScreen;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForJobId;
import static sleeper.clients.admin.JobStatusScreenHelper.promptForRange;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminConfigStore store;
    private final TableSelectHelper tableSelectHelper;

    public IngestStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
    }

    public void chooseArgsAndPrint(String instanceId) throws InterruptedException {
        out.clearScreen("");
        consoleHelper.chooseOptionUntilValid("Which ingest report would you like to run",
                new MenuOption("Ingest Job Status Report", () ->
                        chooseArgsForIngestJobStatusReport(instanceId)),
                new MenuOption("Ingest Task Status Report", () ->
                        chooseArgsForIngestTaskStatusReport(instanceId))
        ).run();
    }

    private void chooseArgsForIngestJobStatusReport(String instanceId) throws InterruptedException {
        Optional<TableProperties> tableOpt = tableSelectHelper.chooseTableOrReturnToMain(instanceId);
        if (tableOpt.isPresent()) {
            String tableName = tableOpt.get().get(TableProperty.TABLE_NAME);
            consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                    new MenuOption("All", () ->
                            runIngestJobStatusReport(instanceId, tableName, JobQuery.Type.ALL)),
                    new MenuOption("Unfinished", () ->
                            runIngestJobStatusReport(instanceId, tableName, JobQuery.Type.UNFINISHED)),
                    new MenuOption("Detailed", () ->
                            runIngestJobStatusReport(instanceId, tableName, JobQuery.Type.DETAILED,
                                    promptForJobId(in))),
                    new MenuOption("Range", () ->
                            runIngestJobStatusReport(instanceId, tableName, JobQuery.Type.RANGE,
                                    promptForRange(in)))
            ).run();
        }
    }

    private void chooseArgsForIngestTaskStatusReport(String instanceId) throws InterruptedException {
        consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                new MenuOption("All", () ->
                        runIngestTaskStatusReport(instanceId, IngestTaskQuery.ALL)),
                new MenuOption("Unfinished", () ->
                        runIngestTaskStatusReport(instanceId, IngestTaskQuery.UNFINISHED))
        ).run();
    }

    private IngestJobStatusReportArguments.Builder argsBuilder(String instanceId, String tableName, JobQuery.Type queryType) {
        return IngestJobStatusReportArguments.builder()
                .instanceId(instanceId).tableName(tableName)
                .queryType(queryType)
                .reporter(new StandardIngestJobStatusReporter(out.printStream()));
    }

    private void runIngestJobStatusReport(String instanceId, String tableName, JobQuery.Type queryType, String queryArgs) {
        runIngestJobStatusReport(instanceId, argsBuilder(instanceId, tableName, queryType)
                .queryParameters(queryArgs).build());
    }

    private void runIngestJobStatusReport(String instanceId, String tableName, JobQuery.Type queryType) {
        runIngestJobStatusReport(instanceId, argsBuilder(instanceId, tableName, queryType).build());
    }

    private void runIngestJobStatusReport(String instanceId, IngestJobStatusReportArguments args) {
        InstanceProperties instanceProperties = store.loadInstanceProperties(instanceId);
        new IngestJobStatusReport(store.loadIngestJobStatusStore(instanceId), args,
                store.getSqsClient(), instanceProperties.get(INGEST_JOB_QUEUE_URL)).run();
        confirmReturnToMainScreen(out, in);
    }

    private void runIngestTaskStatusReport(String instanceId, IngestTaskQuery queryType) {
        new IngestTaskStatusReport(store.loadIngestTaskStatusStore(instanceId),
                new StandardIngestTaskStatusReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}
