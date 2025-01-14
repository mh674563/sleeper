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

package sleeper.clients.docker;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import sleeper.clients.deploy.PopulateInstanceProperties;
import sleeper.clients.deploy.PopulateTableProperties;
import sleeper.clients.docker.stack.ConfigurationDockerStack;
import sleeper.clients.docker.stack.TableDockerStack;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.StringType;

import static sleeper.configuration.properties.instance.CommonProperty.ACCOUNT;
import static sleeper.configuration.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.configuration.properties.instance.CommonProperty.REGION;
import static sleeper.configuration.properties.instance.CommonProperty.SUBNETS;
import static sleeper.configuration.properties.instance.CommonProperty.VPC_ID;
import static sleeper.configuration.utils.AwsV1ClientHelper.buildAwsV1Client;

public class DeployDockerInstance {
    private DeployDockerInstance() {

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: <instance-id>");
        }
        if (System.getenv("AWS_ENDPOINT_URL") == null) {
            throw new IllegalArgumentException("Environment variable AWS_ENDPOINT_URL not set");
        }
        String instanceId = args[0];
        AmazonS3 s3Client = buildAwsV1Client(AmazonS3ClientBuilder.standard());
        AmazonDynamoDB dynamoDB = buildAwsV1Client(AmazonDynamoDBClientBuilder.standard());
        deploy(instanceId, s3Client, dynamoDB);
    }

    public static void deploy(String instanceId, AmazonS3 s3Client, AmazonDynamoDB dynamoDB) throws Exception {
        InstanceProperties instanceProperties = generateInstanceProperties(instanceId);
        TableProperties tableProperties = generateTableProperties(instanceProperties);

        ConfigurationDockerStack.from(instanceProperties, s3Client).deploy();
        TableDockerStack.from(instanceProperties, tableProperties, s3Client, dynamoDB).deploy();

        instanceProperties.saveToS3(s3Client);
        tableProperties.saveToS3(s3Client);
    }

    private static InstanceProperties generateInstanceProperties(String instanceId) {
        InstanceProperties instanceProperties = PopulateInstanceProperties.populateDefaultsFromInstanceId(
                new InstanceProperties(), instanceId);
        instanceProperties.set(OPTIONAL_STACKS, "IngestStack,CompactionStack,PartitionSplittingStack,QueryStack");
        instanceProperties.set(ACCOUNT, "test-account");
        instanceProperties.set(VPC_ID, "test-vpc");
        instanceProperties.set(SUBNETS, "test-subnet");
        instanceProperties.set(REGION, "us-east-1");
        return instanceProperties;
    }

    private static TableProperties generateTableProperties(InstanceProperties instanceProperties) {
        return PopulateTableProperties.builder()
                .tableName("system-test")
                .instanceProperties(instanceProperties)
                .schema(Schema.builder().rowKeyFields(new Field("key", new StringType())).build())
                .build().populate();
    }
}
