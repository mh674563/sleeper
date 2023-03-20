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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.clients.cdk.CdkCommand;
import sleeper.clients.cdk.InvokeCdkForInstance;
import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.InstanceProperty;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.configuration.properties.table.TableProperty;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreProvider;
import sleeper.table.job.TableLister;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static sleeper.configuration.properties.UserDefinedInstanceProperty.ID;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class AdminConfigStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdminConfigStore.class);

    private final AmazonS3 s3;
    private final AmazonDynamoDB dynamoDB;
    private final InvokeCdkForInstance cdk;
    private final Path generatedDirectory;

    public AdminConfigStore(AmazonS3 s3, AmazonDynamoDB dynamoDB, InvokeCdkForInstance cdk, Path generatedDirectory) {
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
        this.cdk = cdk;
        this.generatedDirectory = generatedDirectory;
    }

    public InstanceProperties loadInstanceProperties(String instanceId) {
        InstanceProperties instanceProperties = new InstanceProperties();
        try {
            instanceProperties.loadFromS3GivenInstanceId(s3, instanceId);
        } catch (IOException | AmazonS3Exception e) {
            throw new CouldNotLoadInstanceProperties(instanceId, e);
        }
        return instanceProperties;
    }

    public TableProperties loadTableProperties(String instanceId, String tableName) {
        return loadTableProperties(loadInstanceProperties(instanceId), tableName);
    }

    private TableProperties loadTableProperties(InstanceProperties instanceProperties, String tableName) {
        return new TablePropertiesProvider(s3, instanceProperties).getTableProperties(tableName);
    }

    public List<String> listTables(String instanceId) {
        return listTables(loadInstanceProperties(instanceId));
    }

    private List<String> listTables(InstanceProperties instanceProperties) {
        return new TableLister(s3, instanceProperties).listTables();
    }

    private Stream<TableProperties> streamTableProperties(InstanceProperties instanceProperties) {
        return listTables(instanceProperties).stream()
                .map(tableName -> loadTableProperties(instanceProperties, tableName));
    }

    public void saveInstanceProperties(InstanceProperties properties, PropertiesDiff diff) {
        try {
            LOGGER.info("Saving to local configuration");
            ClientUtils.clearDirectory(generatedDirectory);
            SaveLocalProperties.saveToDirectory(generatedDirectory, properties, streamTableProperties(properties));
            List<InstanceProperty> propertiesDeployedByCdk = diff.getChangedPropertiesDeployedByCDK(properties.getPropertiesIndex());
            if (!propertiesDeployedByCdk.isEmpty()) {
                LOGGER.info("Deploying by CDK, properties requiring CDK deployment: {}", propertiesDeployedByCdk);
                cdk.invokeInferringType(properties, CdkCommand.deployPropertiesChange());
            } else {
                LOGGER.info("Saving to AWS");
                properties.saveToS3(s3);
            }
        } catch (IOException | AmazonS3Exception | InterruptedException e) {
            String instanceId = properties.get(ID);
            CouldNotSaveInstanceProperties wrapped = new CouldNotSaveInstanceProperties(instanceId, e);
            try {
                SaveLocalProperties.saveFromS3(s3, instanceId, generatedDirectory);
            } catch (Exception e2) {
                wrapped.addSuppressed(e2);
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw wrapped;
        }
    }

    public void saveTableProperties(String instanceId, TableProperties properties, PropertiesDiff diff) {
        saveTableProperties(loadInstanceProperties(instanceId), properties, diff);
    }

    public void saveTableProperties(InstanceProperties instanceProperties, TableProperties properties, PropertiesDiff diff) {
        String instanceId = instanceProperties.get(ID);
        String tableName = properties.get(TABLE_NAME);
        try {
            LOGGER.info("Saving to local configuration");
            ClientUtils.clearDirectory(generatedDirectory);
            SaveLocalProperties.saveToDirectory(generatedDirectory, instanceProperties,
                    streamTableProperties(instanceProperties)
                            .map(table -> tableName.equals(table.get(TABLE_NAME))
                                    ? properties : table));
            List<TableProperty> propertiesDeployedByCdk = diff.getChangedPropertiesDeployedByCDK(properties.getPropertiesIndex());
            if (!propertiesDeployedByCdk.isEmpty()) {
                LOGGER.info("Deploying by CDK, properties requiring CDK deployment: {}", propertiesDeployedByCdk);
                cdk.invokeInferringType(instanceProperties, CdkCommand.deployPropertiesChange());
            } else {
                LOGGER.info("Saving to AWS");
                properties.saveToS3(s3);
            }
        } catch (IOException | AmazonS3Exception | InterruptedException e) {
            CouldNotSaveTableProperties wrapped = new CouldNotSaveTableProperties(instanceId, tableName, e);
            try {
                SaveLocalProperties.saveFromS3(s3, instanceId, generatedDirectory);
            } catch (Exception e2) {
                wrapped.addSuppressed(e2);
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw wrapped;
        }
    }

    public StateStore loadStateStore(String instanceId, TableProperties tableProperties) {
        InstanceProperties instanceProperties = loadInstanceProperties(instanceId);
        StateStoreProvider stateStoreProvider = new StateStoreProvider(dynamoDB, instanceProperties, new Configuration());
        return stateStoreProvider.getStateStore(tableProperties);
    }

    public static class CouldNotLoadInstanceProperties extends RuntimeException {
        public CouldNotLoadInstanceProperties(String instanceId, Throwable e) {
            super("Could not load properties for instance " + instanceId, e);
        }
    }

    public static class CouldNotSaveInstanceProperties extends RuntimeException {
        public CouldNotSaveInstanceProperties(String instanceId, Throwable e) {
            super("Could not save properties for instance " + instanceId, e);
        }
    }

    public static class CouldNotSaveTableProperties extends RuntimeException {
        public CouldNotSaveTableProperties(String instanceId, String tableName, Throwable e) {
            super("Could not save properties for table " + tableName + " in instance " + instanceId, e);
        }
    }
}
