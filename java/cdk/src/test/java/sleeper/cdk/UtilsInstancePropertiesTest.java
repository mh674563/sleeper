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
package sleeper.cdk;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import sleeper.configuration.properties.InstanceProperties;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.cdk.Utils.loadInstanceProperties;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.propertiesString;

class UtilsInstancePropertiesTest {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    @TempDir
    private Path tempDir;
    private Path instancePropertiesFile;

    @BeforeEach
    void setUp() throws IOException {
        instancePropertiesFile = tempDir.resolve("instance.properties");
        instanceProperties.save(instancePropertiesFile);
    }

    @Test
    void shouldLoadTagsFromTagsFileNextToInstancePropertiesFile() throws IOException {
        // Given
        Properties tags = new Properties();
        tags.setProperty("tag-1", "value-1");
        Files.writeString(tempDir.resolve("tags.properties"), propertiesString(tags));

        // When
        InstanceProperties loaded = loadInstanceProperties(new InstanceProperties(), instancePropertiesFile);

        // Then
        instanceProperties.setTags(Map.of("tag-1", "value-1"));
        assertThat(loaded).isEqualTo(instanceProperties);
    }

    @Test
    void shouldIgnoreTagFileIfMissingNextToInstancePropertiesFile() throws IOException {
        // Given/When
        InstanceProperties loaded = loadInstanceProperties(new InstanceProperties(), instancePropertiesFile);

        // Then
        assertThat(loaded).isEqualTo(instanceProperties);
    }
}
