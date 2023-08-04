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
package sleeper.ingest.testutils;

import com.facebook.collections.ByteArray;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.iterator.IteratorException;
import sleeper.core.iterator.MergingIterator;
import sleeper.core.key.Key;
import sleeper.core.partition.PartitionTree;
import sleeper.core.record.KeyComparator;
import sleeper.core.record.Record;
import sleeper.core.record.RecordComparator;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.ingest.impl.IngestCoordinator;
import sleeper.io.parquet.record.ParquetReaderIterator;
import sleeper.io.parquet.record.ParquetRecordReader;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;
import sleeper.statestore.FileInfo;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.ingest.impl.IngestCoordinatorCommonIT.AWS_EXTERNAL_RESOURCE;

public class ResultVerifier {
    private static final double QUANTILE_SKETCH_TOLERANCE = 0.01;

    private ResultVerifier() {
    }

    public static Pair<String, Map<Integer, List<Record>>> verify(
            StateStore stateStore,
            java.nio.file.Path temporaryFolder,
            QuinFunction<StateStore, Schema, String, String, java.nio.file.Path, IngestCoordinator<Record>> ingestCoordinatorFactoryFn,
            RecordGenerator.RecordListAndSchema recordListAndSchema,
            Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
            String sleeperIteratorClassName,
            List<Record> expectedRecordsList,
            Function<Key, Integer> keyToPartitionNoMappingFn
    ) throws StateStoreException, IOException, IteratorException {

        String ingestLocalWorkingDirectory = createTempDirectory(temporaryFolder, null).toString() + "/path/to/new/sub/directory";
        try (IngestCoordinator<Record> ingestCoordinator =
                     ingestCoordinatorFactoryFn.apply(
                             stateStore,
                             recordListAndSchema.sleeperSchema,
                             sleeperIteratorClassName,
                             ingestLocalWorkingDirectory,
                             temporaryFolder)) {
            for (Record record : recordListAndSchema.recordList) {
                ingestCoordinator.write(record);
            }
        }
        Map<Integer, List<Record>> allPartitionNoMap = getAllPartitionNoMap(
                recordListAndSchema.sleeperSchema,
                recordListAndSchema.recordList,
                keyToPartitionNoMappingFn
        );
        allPartitionNoMap.keySet().forEach(partitionNo -> {
            try {
                verifyPartition(
                        recordListAndSchema.sleeperSchema,
                        partitionNo,
                        partitionNoToExpectedNoOfFilesMap,
                        AWS_EXTERNAL_RESOURCE.getHadoopConfiguration(),
                        keyToPartitionNoMappingFn,
                        new PartitionTree(recordListAndSchema.sleeperSchema, stateStore.getAllPartitions()),
                        stateStore,
                        expectedRecordsList);
            } catch (StateStoreException e) {
                throw new RuntimeException(e);
            }
        });

        return Pair.of(ingestLocalWorkingDirectory, allPartitionNoMap);
    }

    private static Map<Integer, List<Record>> getAllPartitionNoMap(
            Schema sleeperSchema,
            List<Record> expectedRecords,
            Function<Key, Integer> keyToPartitionNoMappingFn
    ) {
        return expectedRecords.stream()
                .collect(Collectors.groupingBy(
                        record -> keyToPartitionNoMappingFn
                                .apply(Key.create(record.getValues(sleeperSchema.getRowKeyFieldNames())))));
    }


    public static void verifyPartition(Schema sleeperSchema,
                                       Integer partitionNo,
                                       Map<Integer, Integer> partitionNoToExpectedNoOfFilesMap,
                                       Configuration hadoopConfiguration,
                                       Function<Key, Integer> keyToPartitionNoMappingFn,
                                       PartitionTree partitionTree,
                                       StateStore stateStore,
                                       List<Record> expectedRecords
    ) throws StateStoreException {
        Map<Integer, List<Record>> partitionNoToExpectedRecordsMap = expectedRecords.stream()
                .collect(Collectors.groupingBy(
                        record -> keyToPartitionNoMappingFn.apply(Key.create(record.getValues(sleeperSchema.getRowKeyFieldNames())))));
        List<Record> expectedRecordList = partitionNoToExpectedRecordsMap.getOrDefault(partitionNo, Collections.emptyList());
        Integer expectedNoOfFiles = partitionNoToExpectedNoOfFilesMap.get(partitionNo);

        Map<String, Integer> partitionIdToPartitionNoMap = partitionNoToExpectedRecordsMap.entrySet().stream()
                .map(entry -> new AbstractMap.SimpleEntry<>(partitionTree.getLeafPartition(Key.create(entry.getValue().get(0).getValues(sleeperSchema.getRowKeyFieldNames()))).getId(), entry.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        List<FileInfo> partitionFileInfoList = stateStore.getActiveFiles().stream()
                .collect(Collectors.groupingBy(FileInfo::getPartitionId))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> partitionIdToPartitionNoMap.get(entry.getKey()),
                        Map.Entry::getValue)).getOrDefault(partitionNo, Collections.emptyList());


        Comparator<Record> recordComparator = new RecordComparator(sleeperSchema);
        List<Record> expectedSortedRecordList = expectedRecordList.stream()
                .sorted(recordComparator)
                .collect(Collectors.toList());
        List<Record> savedRecordList = readMergedRecordsFromPartitionDataFiles(sleeperSchema, partitionFileInfoList, hadoopConfiguration);

        assertThat(partitionFileInfoList).hasSize(expectedNoOfFiles);
        assertListsIdentical(expectedSortedRecordList, savedRecordList);

        // In some situations, check that the file min and max match the min and max of dimension 0
        if (expectedNoOfFiles == 1 &&
                sleeperSchema.getRowKeyFields().get(0).getType() instanceof LongType) {
            String rowKeyFieldNameDimension0 = sleeperSchema.getRowKeyFieldNames().get(0);
            Key minRowKeyDimension0 = expectedRecordList.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .min(Comparator.naturalOrder())
                    .map(Key::create)
                    .get();
            Key maxRowKeyDimension0 = expectedRecordList.stream()
                    .map(record -> (Long) record.get(rowKeyFieldNameDimension0))
                    .max(Comparator.naturalOrder())
                    .map(Key::create)
                    .get();
            partitionFileInfoList.forEach(fileInfo -> {
                assertThat(fileInfo.getMinRowKey()).isEqualTo(minRowKeyDimension0);
                assertThat(fileInfo.getMaxRowKey()).isEqualTo(maxRowKeyDimension0);
            });
        }

        if (expectedNoOfFiles > 0) {
            Map<Field, ItemsSketch> expectedFieldToItemsSketchMap = createFieldToItemSketchMap(sleeperSchema, expectedRecordList);
            Map<Field, ItemsSketch> savedFieldToItemsSketchMap = readFieldToItemSketchMap(sleeperSchema, partitionFileInfoList, hadoopConfiguration);
            sleeperSchema.getRowKeyFields().forEach(field -> {
                ItemsSketch expectedSketch = expectedFieldToItemsSketchMap.get(field);
                ItemsSketch savedSketch = savedFieldToItemsSketchMap.get(field);
                assertThat(savedSketch.getMinValue()).isEqualTo(expectedSketch.getMinValue());
                assertThat(savedSketch.getMaxValue()).isEqualTo(expectedSketch.getMaxValue());
                IntStream.rangeClosed(0, 10).forEach(quantileNo -> {
                    double quantile = 0.1 * quantileNo;
                    double quantileWithToleranceLower = (quantile - QUANTILE_SKETCH_TOLERANCE) > 0 ? quantile - QUANTILE_SKETCH_TOLERANCE : 0;
                    double quantileWithToleranceUpper = (quantile + QUANTILE_SKETCH_TOLERANCE) < 1 ? quantile + QUANTILE_SKETCH_TOLERANCE : 1;
                    KeyComparator keyComparator = new KeyComparator((PrimitiveType) field.getType());
                    if (field.getType() instanceof ByteArrayType) {
                        assertThat(keyComparator.compare(
                                Key.create(((ByteArray) savedSketch.getQuantile(quantile)).getArray()),
                                Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceLower)).getArray())))
                                .isGreaterThanOrEqualTo(0);
                        assertThat(keyComparator.compare(
                                Key.create(((ByteArray) savedSketch.getQuantile(quantile)).getArray()),
                                Key.create(((ByteArray) expectedSketch.getQuantile(quantileWithToleranceUpper)).getArray())))
                                .isLessThanOrEqualTo(0);
                    } else {
                        assertThat(keyComparator.compare(
                                Key.create(savedSketch.getQuantile(quantile)),
                                Key.create(expectedSketch.getQuantile(quantileWithToleranceLower))))
                                .isGreaterThanOrEqualTo(0);
                        assertThat(keyComparator.compare(
                                Key.create(savedSketch.getQuantile(quantile)),
                                Key.create(expectedSketch.getQuantile(quantileWithToleranceUpper))))
                                .isLessThanOrEqualTo(0);
                    }
                });
            });
        }
    }

    private static void assertListsIdentical(List<?> list1, List<?> list2) {
        assertThat(list2).hasSameSizeAs(list1);
        IntStream.range(0, list1.size()).forEach(i ->
                assertThat(list2.get(i)).as(String.format("First difference found at element %d (of %d)", i, list1.size())).isEqualTo(list1.get(i)));
    }

    private static Map<Field, ItemsSketch> readFieldToItemSketchMap(Schema sleeperSchema,
                                                                    List<FileInfo> partitionFileInfoList,
                                                                    Configuration hadoopConfiguration) {
        List<Sketches> readSketchesList = partitionFileInfoList.stream()
                .map(fileInfo -> {
                    try {
                        String sketchFileName = fileInfo.getFilename().replace(".parquet", ".sketches");
                        return new SketchesSerDeToS3(sleeperSchema).loadFromHadoopFS(new Path(sketchFileName), hadoopConfiguration);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toList());
        Set<String> fieldNameSet = readSketchesList.stream()
                .flatMap(sketches -> sketches.getQuantilesSketches().keySet().stream())
                .collect(Collectors.toSet());
        return fieldNameSet.stream()
                .map(fieldName -> {
                    List<ItemsSketch> itemsSketchList = readSketchesList.stream().map(sketches -> sketches.getQuantilesSketch(fieldName)).collect(Collectors.toList());
                    Field field = sleeperSchema.getField(fieldName).get();
                    return new AbstractMap.SimpleEntry<>(field, mergeSketches(itemsSketchList));
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static ItemsSketch mergeSketches(List<ItemsSketch> itemsSketchList) {
        ItemsUnion union = ItemsUnion.getInstance(1024, Comparator.naturalOrder());
        itemsSketchList.forEach(union::update);
        return union.getResult();
    }

    private static Map<Field, ItemsSketch> createFieldToItemSketchMap(Schema sleeperSchema, List<Record> recordList) {
        return sleeperSchema.getRowKeyFields().stream()
                .map(field -> new AbstractMap.SimpleEntry<>(field, createItemSketch(field, recordList)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static ItemsSketch createItemSketch(Field field, List<Record> recordList) {
        ItemsSketch itemsSketch = ItemsSketch.getInstance(1024, Comparator.naturalOrder());
        if (field.getType() instanceof ByteArrayType) {
            recordList.forEach(record -> itemsSketch.update(ByteArray.wrap((byte[]) record.get(field.getName()))));
        } else {
            recordList.forEach(record -> itemsSketch.update(record.get(field.getName())));
        }
        return itemsSketch;
    }

    private static List<Record> readMergedRecordsFromPartitionDataFiles(Schema sleeperSchema,
                                                                        List<FileInfo> fileInfoList,
                                                                        Configuration hadoopConfiguration) {
        List<CloseableIterator<Record>> inputIterators = fileInfoList.stream()
                .map(fileInfo -> createParquetReaderIterator(
                        sleeperSchema, new Path(fileInfo.getFilename()), hadoopConfiguration))
                .collect(Collectors.toList());
        MergingIterator mergingIterator = new MergingIterator(sleeperSchema, inputIterators);
        List<Record> recordsRead = new ArrayList<>();
        while (mergingIterator.hasNext()) {
            recordsRead.add(mergingIterator.next());
        }
        return recordsRead;
    }

    private static ParquetReaderIterator createParquetReaderIterator(Schema sleeperSchema,
                                                                     Path filePath,
                                                                     Configuration hadoopConfiguration) {
        try {
            ParquetReader<Record> recordParquetReader = new ParquetRecordReader.Builder(filePath, sleeperSchema)
                    .withConf(hadoopConfiguration)
                    .build();
            return new ParquetReaderIterator(recordParquetReader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
