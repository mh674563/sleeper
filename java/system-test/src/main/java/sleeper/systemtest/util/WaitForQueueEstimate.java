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

package sleeper.systemtest.util;

import com.amazonaws.services.sqs.AmazonSQS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJobStatusStore;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.job.common.QueueMessageCount;
import sleeper.util.PollWithRetries;

import java.util.function.Predicate;

public class WaitForQueueEstimate {
    private static final Logger LOGGER = LoggerFactory.getLogger(WaitForQueueEstimate.class);
    private static final long POLL_INTERVAL_MILLIS = 5000;
    private static final int MAX_POLLS = 12;

    private final QueueMessageCount.Client queueClient;
    private final Predicate<QueueMessageCount> isFinished;
    private final String description;
    private final String queueUrl;
    private final PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(POLL_INTERVAL_MILLIS, MAX_POLLS);

    public static WaitForQueueEstimate notEmpty(
            AmazonSQS sqsClient, InstanceProperties instanceProperties, InstanceProperty queueProperty) {
        String queueUrl = instanceProperties.get(queueProperty);
        return new WaitForQueueEstimate(sqsClient, queueUrl,
                estimate -> estimate.getApproximateNumberOfMessages() > 0,
                "estimate not empty for queue " + queueUrl);
    }

    public static WaitForQueueEstimate isEmpty(
            AmazonSQS sqsClient, InstanceProperties instanceProperties, InstanceProperty queueProperty) {
        String queueUrl = instanceProperties.get(queueProperty);
        return new WaitForQueueEstimate(sqsClient, queueUrl,
                estimate -> estimate.getApproximateNumberOfMessages() == 0,
                "estimate empty for queue " + queueUrl);
    }

    public static WaitForQueueEstimate containsUnfinishedJobs(
            AmazonSQS sqsClient, InstanceProperties instanceProperties, InstanceProperty queueProperty,
            CompactionJobStatusStore statusStore, String tableName) {
        int unfinished = statusStore.getUnfinishedJobs(tableName).size();
        LOGGER.info("Found {} unfinished compaction jobs", unfinished);
        return new WaitForQueueEstimate(sqsClient, instanceProperties, queueProperty,
                estimate -> estimate.getApproximateNumberOfMessages() >= unfinished,
                "queue estimate matching unfinished compaction jobs");
    }

    private WaitForQueueEstimate(AmazonSQS sqsClient, InstanceProperties properties, InstanceProperty queueProperty,
                                 Predicate<QueueMessageCount> isFinished, String description) {
        this(sqsClient, properties.get(queueProperty), isFinished, description);
    }

    private WaitForQueueEstimate(AmazonSQS sqsClient, String queueUrl,
                                 Predicate<QueueMessageCount> isFinished, String description) {
        this.queueClient = QueueMessageCount.withSqsClient(sqsClient);
        this.queueUrl = queueUrl;
        this.isFinished = isFinished;
        this.description = description;
    }

    public void pollUntilFinished() throws InterruptedException {
        LOGGER.info("Waiting for messages on {}", queueUrl);
        poll.pollUntil(description, this::isFinished);
    }

    private boolean isFinished() {
        QueueMessageCount count = queueClient.getQueueMessageCount(queueUrl);
        LOGGER.info("Message count: {}", count);
        return isFinished.test(count);
    }
}