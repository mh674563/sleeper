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
import sleeper.job.common.QueueMessageCount;
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
import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_STATUS_STORE_ENABLED;

public class IngestStatusReportScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ConsoleHelper consoleHelper;
    private final AdminConfigStore store;
    private final QueueMessageCount.Client queueClient;
    private final TableSelectHelper tableSelectHelper;

    public IngestStatusReportScreen(ConsoleOutput out, ConsoleInput in, AdminConfigStore store,
                                    QueueMessageCount.Client queueClient) {
        this.out = out;
        this.in = in;
        this.consoleHelper = new ConsoleHelper(out, in);
        this.store = store;
        this.queueClient = queueClient;
        this.tableSelectHelper = new TableSelectHelper(out, in, store);
    }

    public void chooseArgsAndPrint(String instanceId) throws InterruptedException {
        InstanceProperties properties = store.loadInstanceProperties(instanceId);
        if (!properties.getBoolean(INGEST_STATUS_STORE_ENABLED)) {
            out.println("");
            out.println("Ingest status store not enabled. Please enable in instance properties to access this screen");
            confirmReturnToMainScreen(out, in);
        } else {
            out.clearScreen("");
            consoleHelper.chooseOptionUntilValid("Which ingest report would you like to run",
                    new MenuOption("Ingest Job Status Report", () ->
                            chooseArgsForIngestJobStatusReport(properties)),
                    new MenuOption("Ingest Task Status Report", () ->
                            chooseArgsForIngestTaskStatusReport(properties.get(ID)))
            ).run();
        }
    }

    private void chooseArgsForIngestJobStatusReport(InstanceProperties properties) throws InterruptedException {
        Optional<TableProperties> tableOpt = tableSelectHelper.chooseTableOrReturnToMain(properties.get(ID));
        if (tableOpt.isPresent()) {
            String tableName = tableOpt.get().get(TableProperty.TABLE_NAME);
            IngestJobStatusReportArguments.Builder argsBuilder = IngestJobStatusReportArguments.builder()
                    .instanceId(properties.get(ID)).tableName(tableName)
                    .reporter(new StandardIngestJobStatusReporter(out.printStream()));
            consoleHelper.chooseOptionUntilValid("Which query type would you like to use",
                    new MenuOption("All", () ->
                            runIngestJobStatusReport(properties, argsBuilder.queryType(JobQuery.Type.ALL).build())),
                    new MenuOption("Unfinished", () ->
                            runIngestJobStatusReport(properties, argsBuilder.queryType(JobQuery.Type.UNFINISHED).build())),
                    new MenuOption("Detailed", () ->
                            runIngestJobStatusReport(properties, argsBuilder.queryType(JobQuery.Type.DETAILED)
                                    .queryParameters(promptForJobId(in)).build())),
                    new MenuOption("Range", () ->
                            runIngestJobStatusReport(properties, argsBuilder.queryType(JobQuery.Type.RANGE)
                                    .queryParameters(promptForRange(in)).build()))
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

    private void runIngestJobStatusReport(InstanceProperties properties, IngestJobStatusReportArguments args) {
        new IngestJobStatusReport(store.loadIngestJobStatusStore(properties.get(ID)), args,
                queueClient, properties).run();
        confirmReturnToMainScreen(out, in);
    }

    private void runIngestTaskStatusReport(String instanceId, IngestTaskQuery queryType) {
        new IngestTaskStatusReport(store.loadIngestTaskStatusStore(instanceId),
                new StandardIngestTaskStatusReporter(out.printStream()), queryType).run();
        confirmReturnToMainScreen(out, in);
    }
}