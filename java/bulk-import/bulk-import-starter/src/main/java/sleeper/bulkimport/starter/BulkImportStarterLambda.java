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
package sleeper.bulkimport.starter;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.BulkImportExecutor;
import sleeper.bulkimport.starter.executor.PlatformExecutor;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.ingest.status.store.job.DynamoDBIngestJobStatusStore;
import sleeper.statestore.StateStoreProvider;
import sleeper.utils.HadoopPathUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.CONFIG_BUCKET;

/**
 * The {@link BulkImportStarterLambda} consumes {@link BulkImportJob} messages from SQS and starts executes them using
 * an {@link BulkImportExecutor}.
 */
public class BulkImportStarterLambda implements RequestHandler<SQSEvent, Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportStarterLambda.class);

    private final BulkImportExecutor executor;
    private final Configuration hadoopConfig;
    private final BulkImportJobSerDe bulkImportJobSerDe = new BulkImportJobSerDe();
    private final InstanceProperties instanceProperties;

    public BulkImportStarterLambda() throws IOException {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        AmazonDynamoDB dynamo = AmazonDynamoDBClientBuilder.defaultClient();
        instanceProperties = loadInstanceProperties(s3);
        TablePropertiesProvider tablePropertiesProvider = new TablePropertiesProvider(s3, instanceProperties);
        PlatformExecutor platformExecutor = PlatformExecutor.fromEnvironment(
                instanceProperties, tablePropertiesProvider);
        hadoopConfig = new Configuration();
        executor = new BulkImportExecutor(instanceProperties, tablePropertiesProvider,
                new StateStoreProvider(dynamo, instanceProperties),
                new DynamoDBIngestJobStatusStore(dynamo, instanceProperties),
                s3, platformExecutor, Instant::now);
    }

    public BulkImportStarterLambda(BulkImportExecutor executor, InstanceProperties properties, Configuration hadoopConfig) {
        this.executor = executor;
        this.instanceProperties = properties;
        this.hadoopConfig = hadoopConfig;
    }

    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        LOGGER.info("Received request: {}", event);
        event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .map(bulkImportJobSerDe::fromJson)
                .map(this::expandDirectories)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(executor::runJob);
        return null;
    }

    private Optional<BulkImportJob> expandDirectories(BulkImportJob job) {
        BulkImportJob.Builder builder = job.toBuilder();
        List<String> files = HadoopPathUtils.expandDirectories(job.getFiles(), hadoopConfig, instanceProperties);
        if (files.isEmpty()) {
            LOGGER.warn("Could not find files for job: {}", job);
            return Optional.empty();
        }
        return Optional.of(builder.files(files).build());
    }

    private static InstanceProperties loadInstanceProperties(AmazonS3 s3Client) throws IOException {
        InstanceProperties instanceProperties = new InstanceProperties();
        instanceProperties.loadFromS3(s3Client, System.getenv(CONFIG_BUCKET.toEnvironmentVariable()));
        return instanceProperties;
    }
}