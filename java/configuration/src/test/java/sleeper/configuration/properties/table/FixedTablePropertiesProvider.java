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
package sleeper.configuration.properties.table;

import java.util.Objects;

import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class FixedTablePropertiesProvider extends TablePropertiesProvider {

    public FixedTablePropertiesProvider(TableProperties tableProperties) {
        super(tableName -> {
            if (Objects.equals(tableName, tableProperties.get(TABLE_NAME))) {
                return tableProperties;
            } else {
                throw new IllegalArgumentException("Table not found: " + tableName);
            }
        });
    }
}