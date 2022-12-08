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
package sleeper.ingest.job;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleeper.configuration.jars.ObjectFactory;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.UserDefinedInstanceProperty;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.iterator.IteratorException;
import sleeper.ingest.IngestResult;
import sleeper.job.common.action.ActionException;
import sleeper.job.common.action.DeleteMessageAction;
import sleeper.job.common.action.MessageReference;
import sleeper.job.common.action.thread.PeriodicActionRunnable;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.util.List;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS;
import static sleeper.configuration.properties.UserDefinedInstanceProperty.S3A_INPUT_FADVISE;

/**
 * An IngestJobQueueConsumer pulls ingest jobs off an SQS queue and runs them.
 */
public class IngestJobQueueConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestJobQueueConsumer.class);

    private final AmazonSQS sqsClient;
    private final AmazonCloudWatch cloudWatchClient;
    private final InstanceProperties instanceProperties;
    private final String sqsJobQueueUrl;
    private final int keepAlivePeriod;
    private final int visibilityTimeoutInSeconds;
    private final IngestJobSerDe ingestJobSerDe;
    private final IngestJobRunner ingestJobRunner;

    public IngestJobQueueConsumer(ObjectFactory objectFactory,
                                  AmazonSQS sqsClient,
                                  AmazonCloudWatch cloudWatchClient,
                                  InstanceProperties instanceProperties,
                                  TablePropertiesProvider tablePropertiesProvider,
                                  StateStoreProvider stateStoreProvider,
                                  String localDir) {
        this(objectFactory,
                sqsClient,
                cloudWatchClient,
                instanceProperties,
                tablePropertiesProvider,
                stateStoreProvider,
                localDir,
                S3AsyncClient.create(),
                defaultHadoopConfiguration(instanceProperties.get(S3A_INPUT_FADVISE)));
    }

    public IngestJobQueueConsumer(ObjectFactory objectFactory,
                                  AmazonSQS sqsClient,
                                  AmazonCloudWatch cloudWatchClient,
                                  InstanceProperties instanceProperties,
                                  TablePropertiesProvider tablePropertiesProvider,
                                  StateStoreProvider stateStoreProvider,
                                  String localDir,
                                  S3AsyncClient s3AsyncClient,
                                  Configuration hadoopConfiguration) {
        this.sqsClient = sqsClient;
        this.cloudWatchClient = cloudWatchClient;
        this.instanceProperties = instanceProperties;
        this.sqsJobQueueUrl = instanceProperties.get(INGEST_JOB_QUEUE_URL);
        this.keepAlivePeriod = instanceProperties.getInt(INGEST_KEEP_ALIVE_PERIOD_IN_SECONDS);
        this.visibilityTimeoutInSeconds = instanceProperties.getInt(QUEUE_VISIBILITY_TIMEOUT_IN_SECONDS);
        this.ingestJobSerDe = new IngestJobSerDe();
        this.ingestJobRunner = new IngestJobRunner(
                objectFactory,
                instanceProperties,
                tablePropertiesProvider,
                stateStoreProvider,
                localDir,
                s3AsyncClient,
                hadoopConfiguration);
    }

    private static Configuration defaultHadoopConfiguration(String fadvise) {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.connection.maximum", "10");
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper");
        conf.set("fs.s3a.experimental.input.fadvise", fadvise);
        return conf;
    }

    public void run() throws InterruptedException, IOException, StateStoreException, IteratorException {
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsJobQueueUrl)
                    .withMaxNumberOfMessages(1)
                    .withWaitTimeSeconds(20); // Must be >= 0 and <= 20
            ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);
            List<Message> messages = receiveMessageResult.getMessages();
            if (messages.isEmpty()) {
                LOGGER.info("Finishing as no jobs have been received");
                return;
            }
            LOGGER.info("Received message {}", messages.get(0).getBody());
            IngestJob ingestJob = ingestJobSerDe.fromJson(messages.get(0).getBody());
            LOGGER.info("Deserialised message to ingest job {}", ingestJob);

            long recordsWritten = ingest(ingestJob, messages.get(0).getReceiptHandle());
            LOGGER.info("{} records were written", recordsWritten);
        }
    }

    public long ingest(IngestJob job, String receiptHandle) throws InterruptedException, IteratorException, StateStoreException, IOException {
        // Create background thread to keep messages alive
        MessageReference messageReference = new MessageReference(sqsClient, sqsJobQueueUrl, "Ingest job " + job.getId(), receiptHandle);
        PeriodicActionRunnable changeTimeoutRunnable = new PeriodicActionRunnable(
                messageReference.changeVisibilityTimeoutAction(visibilityTimeoutInSeconds), keepAlivePeriod);
        changeTimeoutRunnable.start();
        LOGGER.info("Ingest job {}: Created background thread to keep SQS messages alive (period is {} seconds)",
                job.getId(), keepAlivePeriod);

        // Run the ingest
        IngestResult result = ingestJobRunner.ingest(job);
        LOGGER.info("Ingest job {}: Stopping background thread to keep SQS messages alive",
                job.getId());
        changeTimeoutRunnable.stop();

        // Delete messages from SQS queue
        LOGGER.info("Ingest job {}: Deleting messages from queue", job.getId());
        DeleteMessageAction deleteAction = messageReference.deleteAction();
        try {
            deleteAction.call();
        } catch (ActionException e) {
            LOGGER.error("Ingest job {}: ActionException deleting message with handle {}", job.getId(), receiptHandle);
        }

        // Update metrics
        String metricsNamespace = instanceProperties.get(UserDefinedInstanceProperty.METRICS_NAMESPACE);
        String instanceId = instanceProperties.get(UserDefinedInstanceProperty.ID);
        cloudWatchClient.putMetricData(new PutMetricDataRequest()
                .withNamespace(metricsNamespace)
                .withMetricData(new MetricDatum()
                        .withMetricName("StandardIngestRecordsWritten")
                        .withValue((double) result.getNumberOfRecords())
                        .withUnit(StandardUnit.Count)
                        .withDimensions(
                                new Dimension().withName("instanceId").withValue(instanceId),
                                new Dimension().withName("tableName").withValue(job.getTableName())
                        )));

        return result.getNumberOfRecords();
    }
}