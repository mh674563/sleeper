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

package sleeper.clients.deploy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.deploy.PopulateInstanceProperties.generateTearDownDefaultsFromInstanceId;
import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.ID;
import static sleeper.configuration.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.properties.instance.CompactionProperty.ECR_COMPACTION_REPO;
import static sleeper.configuration.properties.instance.EKSProperty.BULK_IMPORT_REPO;
import static sleeper.configuration.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO;
import static sleeper.configuration.properties.instance.IngestProperty.ECR_INGEST_REPO;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.CONFIG_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.INGEST_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.PARTITION_SPLITTING_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE;
import static sleeper.configuration.properties.table.TableProperty.SCHEMA;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;
import static sleeper.core.schema.SchemaTestHelper.schemaWithKey;

class PopulatePropertiesTest {
    @Test
    void shouldPopulateInstanceProperties() {
        // Given/When
        InstanceProperties properties = populateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().populate();

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.setTags(Map.of("InstanceID", "test-instance"));
        expected.set(ID, "test-instance");
        expected.set(CONFIG_BUCKET, "sleeper-test-instance-config");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(QUERY_RESULTS_BUCKET, "sleeper-test-instance-query-results");
        expected.set(VPC_ID, "some-vpc");
        expected.set(SUBNETS, "some-subnet");
        expected.set(ECR_COMPACTION_REPO, "test-instance/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-instance/ingest");
        expected.set(BULK_IMPORT_REPO, "test-instance/bulk-import-runner");
        expected.set(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, "test-instance/bulk-import-runner-emr-serverless");
        expected.set(ACCOUNT, "test-account-id");
        expected.set(REGION, "aws-global");

        assertThat(properties).isEqualTo(expected);
    }

    @Test
    void shouldDefaultTagsWhenNotProvidedAndNotSetInInstanceProperties() {
        // Given/When
        InstanceProperties properties = populateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().populate();

        // Then
        assertThat(properties.getTags())
                .isEqualTo(Map.of("InstanceID", "test-instance"));
    }

    @Test
    void shouldAddToExistingTagsWhenSetInInstanceProperties() {
        // Given/When

        InstanceProperties beforePopulate = new InstanceProperties();
        beforePopulate.setTags(Map.of("TestTag", "TestValue"));
        InstanceProperties afterPopulate = populateInstancePropertiesBuilder()
                .instanceProperties(beforePopulate)
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().populate();

        // Then
        assertThat(afterPopulate.getTags())
                .isEqualTo(Map.of("TestTag", "TestValue",
                        "InstanceID", "test-instance"));
    }

    @Test
    void shouldGenerateDefaultInstancePropertiesFromInstanceId() {
        // Given/When
        InstanceProperties properties = generateTearDownDefaultsFromInstanceId("test-instance");

        // Then
        InstanceProperties expected = new InstanceProperties();
        expected.set(ID, "test-instance");
        expected.set(CONFIG_BUCKET, "sleeper-test-instance-config");
        expected.set(JARS_BUCKET, "sleeper-test-instance-jars");
        expected.set(QUERY_RESULTS_BUCKET, "sleeper-test-instance-query-results");
        expected.set(ECR_COMPACTION_REPO, "test-instance/compaction-job-execution");
        expected.set(ECR_INGEST_REPO, "test-instance/ingest");
        expected.set(BULK_IMPORT_REPO, "test-instance/bulk-import-runner");
        expected.set(BULK_IMPORT_EMR_SERVERLESS_CUSTOM_IMAGE_REPO, "test-instance/bulk-import-runner-emr-serverless");
        expected.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-instance-CompactionJobCreationRule");
        expected.set(COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-instance-CompactionTasksCreationRule");
        expected.set(SPLITTING_COMPACTION_TASK_CREATION_CLOUDWATCH_RULE, "test-instance-SplittingCompactionTasksCreationRule");
        expected.set(PARTITION_SPLITTING_CLOUDWATCH_RULE, "test-instance-FindPartitionsToSplitPeriodicTrigger");
        expected.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-instance-GarbageCollectorPeriodicTrigger");
        expected.set(INGEST_CLOUDWATCH_RULE, "test-instance-IngestTasksCreationRule");
        expected.set(INGEST_BATCHER_JOB_CREATION_CLOUDWATCH_RULE, "test-instance-IngestBatcherJobCreationRule");

        assertThat(properties).isEqualTo(expected);
    }


    @Test
    void shouldGenerateTablePropertiesCorrectly() {
        // Given
        InstanceProperties instanceProperties = populateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().populate();
        TableProperties tableProperties = PopulateTableProperties.builder()
                .instanceProperties(instanceProperties)
                .tableProperties(new TableProperties(instanceProperties))
                .schema(schemaWithKey("key"))
                .tableName("test-table")
                .build().populate();

        // Then
        TableProperties expected = new TableProperties(instanceProperties);
        expected.setSchema(schemaWithKey("key"));
        expected.set(TABLE_NAME, "test-table");

        assertThat(tableProperties).isEqualTo(expected);
    }

    @Test
    void shouldRetainWhitespaceInSchema() {
        // Given
        InstanceProperties instanceProperties = populateInstancePropertiesBuilder()
                .instanceId("test-instance").vpcId("some-vpc").subnetIds("some-subnet")
                .build().populate();
        String schemaWithNewlines = "{\"rowKeyFields\":[{\n" +
                "\"name\":\"key\",\"type\":\"LongType\"\n" +
                "}],\n" +
                "\"sortKeyFields\":[],\n" +
                "\"valueFields\":[]}";
        TableProperties tableProperties = PopulateTableProperties.builder()
                .instanceProperties(instanceProperties)
                .schema(schemaWithNewlines)
                .tableProperties(new TableProperties(instanceProperties))
                .tableName("test-table")
                .build().populate();

        // Then
        TableProperties expected = new TableProperties(instanceProperties);
        expected.setSchema(schemaWithKey("key"));
        expected.set(SCHEMA, schemaWithNewlines);
        expected.set(TABLE_NAME, "test-table");

        assertThat(tableProperties).isEqualTo(expected);
    }

    private PopulateInstanceProperties.Builder populateInstancePropertiesBuilder() {
        return PopulateInstanceProperties.builder()
                .accountSupplier(() -> "test-account-id").regionProvider(() -> Region.AWS_GLOBAL);
    }
}
