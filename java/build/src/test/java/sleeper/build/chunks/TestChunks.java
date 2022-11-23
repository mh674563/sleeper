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
package sleeper.build.chunks;

import sleeper.build.testutil.TestResources;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

public class TestChunks {

    private TestChunks() {
    }

    public static ProjectChunks example() {
        return new ProjectChunks(Arrays.asList(bulkImport(), common(), ingest()));
    }

    public static ProjectChunk bulkImport() {
        return ProjectChunk.chunk("bulk-import").name("Bulk Import")
                .workflow("chunk-bulk-import.yaml")
                .modulesArray(
                        "bulk-import/bulk-import-common",
                        "bulk-import/bulk-import-starter",
                        "bulk-import/bulk-import-runner")
                .build();
    }

    public static ProjectChunk common() {
        return ProjectChunk.chunk("common").name("Common")
                .workflow("chunk-common.yaml")
                .modulesArray("core", "configuration")
                .build();
    }

    public static ProjectChunk ingest() {
        return ProjectChunk.chunk("ingest").name("Ingest")
                .workflow("chunk-ingest.yaml")
                .modulesArray("ingest")
                .build();
    }

    public static ProjectChunks example(String path) {
        try (Reader chunksReader = TestResources.exampleReader(path)) {
            return ProjectChunksYaml.read(chunksReader);
        } catch (IOException e) {
            throw new IllegalStateException("Could not load example chunks", e);
        }
    }
}
