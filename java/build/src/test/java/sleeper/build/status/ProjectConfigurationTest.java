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
package sleeper.build.status;

import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class ProjectConfigurationTest {

    @Test
    public void shouldLoadPropertiesForSeveralChunks() {
        Properties properties = TestProperties.example("github-example.properties");
        ProjectConfiguration configuration = ProjectConfiguration.from(properties);

        assertThat(configuration).isEqualTo(
                ProjectConfiguration.builder()
                        .token("test-token")
                        .head(GitHubHead.builder()
                                .owner("test-owner").repository("test-repo").branch("test-branch").sha("test-sha")
                                .build())
                        .chunks(Arrays.asList(
                                ProjectChunk.chunk("common").name("Common").workflow("chunk-common.yaml").build(),
                                ProjectChunk.chunk("data").name("Data").workflow("chunk-data.yaml").build()))
                        .build());
    }
}