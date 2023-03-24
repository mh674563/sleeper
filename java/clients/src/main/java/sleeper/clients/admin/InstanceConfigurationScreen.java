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

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.PropertyGroup;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.console.ConsoleInput;
import sleeper.console.ConsoleOutput;
import sleeper.console.menu.ChooseOne;
import sleeper.console.menu.MenuOption;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Set;

public class InstanceConfigurationScreen {
    private final ConsoleOutput out;
    private final ConsoleInput in;
    private final ChooseOne chooseOne;
    private final PropertyGroupSelectHelper selectGroup;
    private final TableSelectHelper selectTable;
    private final AdminClientPropertiesStore store;
    private final UpdatePropertiesWithNano editor;

    public InstanceConfigurationScreen(ConsoleOutput out, ConsoleInput in, AdminClientPropertiesStore store, UpdatePropertiesWithNano editor) {
        this.out = out;
        this.in = in;
        this.chooseOne = new ChooseOne(out, in);
        this.selectGroup = new PropertyGroupSelectHelper(out, in);
        this.selectTable = new TableSelectHelper(out, in, store);
        this.store = store;
        this.editor = editor;
    }

    public void viewAndEditProperties(String instanceId) throws InterruptedException {
        withInstanceProperties(store.loadInstanceProperties(instanceId))
                .viewAndEditProperties();
    }

    public void viewAndEditTableProperties(String instanceId) throws InterruptedException {
        Optional<TableProperties> tableOpt = selectTable.chooseTableOrReturnToMain(instanceId);
        if (tableOpt.isPresent()) {
            withTableProperties(instanceId, tableOpt.get())
                    .viewAndEditProperties();
        }
    }

    public void viewAndEditPropertyGroup(String instanceId) throws InterruptedException {
        Optional<WithProperties<?>> withProperties = selectGroup.selectPropertyGroup()
                .flatMap(group -> withPropertyGroup(instanceId, group));
        if (withProperties.isPresent()) {
            withProperties.get().viewAndEditProperties();
        }
    }

    private Optional<WithProperties<?>> withPropertyGroup(String instanceId, PropertyGroupWithCategory group) {
        if (group.isInstancePropertyGroup()) {
            return Optional.of(withGroupedInstanceProperties(
                    store.loadInstanceProperties(instanceId), group.getGroup()));
        } else if (group.isTablePropertyGroup()) {
            return selectTable.chooseTableOrReturnToMain(instanceId)
                    .map(table -> withGroupedTableProperties(instanceId, table, group.getGroup()));
        }
        return Optional.empty();
    }

    private WithProperties<InstanceProperties> withInstanceProperties(InstanceProperties properties) {
        return new WithProperties<>(properties, editor::openPropertiesFile, store::saveInstanceProperties);
    }

    private WithProperties<InstanceProperties> withGroupedInstanceProperties(InstanceProperties properties, PropertyGroup group) {
        return new WithProperties<>(properties, props -> editor.openPropertiesFile(props, group), store::saveInstanceProperties);
    }

    private WithProperties<TableProperties> withTableProperties(String instanceId, TableProperties properties) {
        return new WithProperties<>(properties, editor::openPropertiesFile,
                (tableProperties, diff) -> store.saveTableProperties(instanceId, tableProperties, diff));
    }

    private WithProperties<TableProperties> withGroupedTableProperties(
            String instanceId, TableProperties properties, PropertyGroup group) {
        return new WithProperties<>(properties, props -> editor.openPropertiesFile(props, group),
                (tableProperties, diff) -> store.saveTableProperties(instanceId, tableProperties, diff));
    }

    private interface OpenFile<T extends SleeperProperties<?>> {
        UpdatePropertiesRequest<T> openFile(T properties) throws IOException, InterruptedException;
    }

    private interface SaveChanges<T extends SleeperProperties<?>> {
        void saveChanges(T properties, PropertiesDiff diff);
    }

    private class WithProperties<T extends SleeperProperties<?>> {

        private final T properties;
        private final OpenFile<T> editor;
        private final SaveChanges<T> store;

        WithProperties(T properties, OpenFile<T> editor, SaveChanges<T> store) {
            this.properties = properties;
            this.editor = editor;
            this.store = store;
        }

        WithProperties<T> withProperties(T properties) {
            return new WithProperties<>(properties, editor, store);
        }

        void viewAndEditProperties() throws InterruptedException {
            viewAndEditProperties(PropertiesDiff.noChanges());
        }

        void viewAndEditProperties(PropertiesDiff changesSoFar) throws InterruptedException {
            UpdatePropertiesRequest<T> request = openPropertiesFile();
            PropertiesDiff changes = changesSoFar.andThen(request.getDiff());
            if (changes.isChanged()) {
                Set<SleeperProperty> invalidProperties = request.getInvalidProperties();
                changes.print(out, properties.getPropertiesIndex(), invalidProperties);

                chooseFromOptions(request.getUpdatedProperties(), changes, invalidProperties.isEmpty());
            }
        }

        void chooseFromOptions(
                T updatedProperties, PropertiesDiff changes, boolean valid) throws InterruptedException {
            MenuOption saveChanges = new MenuOption("Save changes", () -> {
                try {
                    store.saveChanges(updatedProperties, changes);
                    out.println("\n\n----------------------------------");
                    out.println("Saved successfully, hit enter to return to main screen");
                    in.waitForLine();
                } catch (AdminClientPropertiesStore.CouldNotSaveProperties e) {
                    out.println("\n\n----------------------------------\n");
                    e.print(out);
                    out.println();
                    chooseFromOptions(updatedProperties, changes, valid);
                }
            });
            MenuOption returnToEditor = new MenuOption("Return to editor", () ->
                    withProperties(updatedProperties).viewAndEditProperties(changes));
            MenuOption discardChanges = new MenuOption("Discard changes and return to main menu", () -> {
            });
            if (valid) {
                chooseOne.chooseFrom(saveChanges, returnToEditor, discardChanges)
                        .getChoice().orElse(returnToEditor).run();
            } else {
                chooseOne.chooseFrom(returnToEditor, discardChanges)
                        .getChoice().orElse(returnToEditor).run();
            }
        }

        UpdatePropertiesRequest<T> openPropertiesFile() throws InterruptedException {
            try {
                return editor.openFile(properties);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
