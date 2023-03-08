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

import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.providers.AwsRegionProvider;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.s3.S3Client;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.local.SaveLocalProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.util.ClientUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SleeperProperties.loadProperties;

public class DeployNewInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeployNewInstance.class);

    private final AWSSecurityTokenService sts;
    private final AwsRegionProvider regionProvider;
    private final S3Client s3;
    private final Path scriptsDirectory;
    private final String instanceId;
    private final String vpcId;
    private final String subnetId;
    private final String tableName;
    private final Path instancePropertiesTemplate;
    private final Consumer<Properties> extraInstanceProperties;
    private final CdkDeployInstance.Type instanceType;

    private DeployNewInstance(Builder builder) {
        sts = builder.sts;
        regionProvider = builder.regionProvider;
        s3 = builder.s3;
        scriptsDirectory = builder.scriptsDirectory;
        instanceId = builder.instanceId;
        vpcId = builder.vpcId;
        subnetId = builder.subnetId;
        tableName = builder.tableName;
        instancePropertiesTemplate = builder.instancePropertiesTemplate;
        extraInstanceProperties = builder.extraInstanceProperties;
        instanceType = builder.instanceType;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (5 != args.length) {
            throw new IllegalArgumentException("Usage: <scripts-dir> <instance-id> <vpc> <subnet> <table-name>");
        }
        Path scriptsDirectory = Path.of(args[0]);

        builder().scriptsDirectory(scriptsDirectory)
                .instanceId(args[1])
                .vpcId(args[2])
                .subnetId(args[3])
                .tableName(args[4])
                .instancePropertiesTemplate(scriptsDirectory.resolve("templates/instanceproperties.template"))
                .instanceType(CdkDeployInstance.Type.STANDARD)
                .deployWithDefaultClients();
    }

    public void deploy() throws IOException, InterruptedException {
        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Running Deployment");
        LOGGER.info("-------------------------------------------------------");

        Path templatesDirectory = scriptsDirectory.resolve("templates");
        Path generatedDirectory = scriptsDirectory.resolve("generated");
        Path jarsDirectory = scriptsDirectory.resolve("jars");
        String sleeperVersion = Files.readString(templatesDirectory.resolve("version.txt"));

        LOGGER.info("instanceId: {}", instanceId);
        LOGGER.info("vpcId: {}", vpcId);
        LOGGER.info("subnetId: {}", subnetId);
        LOGGER.info("tableName: {}", tableName);
        LOGGER.info("templatesDirectory: {}", templatesDirectory);
        LOGGER.info("generatedDirectory: {}", generatedDirectory);
        LOGGER.info("instancePropertiesTemplate: {}", instancePropertiesTemplate);
        LOGGER.info("scriptsDirectory: {}", scriptsDirectory);
        LOGGER.info("jarsDirectory: {}", jarsDirectory);
        LOGGER.info("sleeperVersion: {}", sleeperVersion);


        InstanceProperties instanceProperties = GenerateInstanceProperties.builder()
                .sts(sts).regionProvider(regionProvider)
                .properties(loadInstancePropertiesTemplate())
                .tagsProperties(loadProperties(templatesDirectory.resolve("tags.template")))
                .instanceId(instanceId).vpcId(vpcId).subnetId(subnetId)
                .build().generate();
        TableProperties tableProperties = GenerateTableProperties.from(instanceProperties,
                Schema.load(templatesDirectory.resolve("schema.template")),
                loadProperties(templatesDirectory.resolve("tableproperties.template")),
                tableName);
        boolean jarsChanged = SyncJars.builder().s3(s3)
                .jarsDirectory(jarsDirectory).instanceProperties(instanceProperties)
                .deleteOldJars(false).build().sync();
        UploadDockerImages.builder()
                .baseDockerDirectory(scriptsDirectory.resolve("docker"))
                .uploadDockerImagesScript(scriptsDirectory.resolve("deploy/uploadDockerImages.sh"))
                .skipIf(!jarsChanged)
                .instanceProperties(instanceProperties)
                .build().upload();

        Files.createDirectories(generatedDirectory);
        ClientUtils.clearDirectory(generatedDirectory);
        SaveLocalProperties.saveToDirectory(generatedDirectory, instanceProperties, Stream.of(tableProperties));

        LOGGER.info("-------------------------------------------------------");
        LOGGER.info("Deploying Stacks");
        LOGGER.info("-------------------------------------------------------");
        CdkDeployInstance.builder()
                .instancePropertiesFile(generatedDirectory.resolve("instance.properties"))
                .jarsDirectory(jarsDirectory).version(sleeperVersion)
                .ensureNewInstance(true)
                .build().deploy(instanceType);
        LOGGER.info("Finished deployment of new instance");
    }

    private Properties loadInstancePropertiesTemplate() throws IOException {
        Properties properties = loadProperties(instancePropertiesTemplate);
        extraInstanceProperties.accept(properties);
        return properties;
    }

    public static final class Builder {
        private AWSSecurityTokenService sts;
        private AwsRegionProvider regionProvider;
        private S3Client s3;
        private Path scriptsDirectory;
        private String instanceId;
        private String vpcId;
        private String subnetId;
        private String tableName;
        private Path instancePropertiesTemplate;
        private Consumer<Properties> extraInstanceProperties = properties -> {
        };
        private CdkDeployInstance.Type instanceType;

        private Builder() {
        }

        public Builder sts(AWSSecurityTokenService sts) {
            this.sts = sts;
            return this;
        }

        public Builder regionProvider(AwsRegionProvider regionProvider) {
            this.regionProvider = regionProvider;
            return this;
        }

        public Builder s3(S3Client s3) {
            this.s3 = s3;
            return this;
        }

        public Builder scriptsDirectory(Path scriptsDirectory) {
            this.scriptsDirectory = scriptsDirectory;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder vpcId(String vpcId) {
            this.vpcId = vpcId;
            return this;
        }

        public Builder subnetId(String subnetId) {
            this.subnetId = subnetId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder instancePropertiesTemplate(Path instancePropertiesTemplate) {
            this.instancePropertiesTemplate = instancePropertiesTemplate;
            return this;
        }

        public Builder extraInstanceProperties(Consumer<Properties> extraInstanceProperties) {
            this.extraInstanceProperties = extraInstanceProperties;
            return this;
        }

        public Builder instanceType(CdkDeployInstance.Type instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public DeployNewInstance build() {
            return new DeployNewInstance(this);
        }

        public void deployWithDefaultClients() throws IOException, InterruptedException {

            try (S3Client s3Client = S3Client.create()) {
                sts(AWSSecurityTokenServiceClientBuilder.defaultClient());
                regionProvider(DefaultAwsRegionProviderChain.builder().build());
                s3(s3Client);
                build().deploy();
            }
        }
    }
}