/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.schema;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.factory.FieldAggregatorFactory;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.BUCKET_KEY;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.CHANGELOG_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.DEFAULT_AGG_FUNCTION;
import static org.apache.paimon.CoreOptions.FIELDS_PREFIX;
import static org.apache.paimon.CoreOptions.FIELDS_SEPARATOR;
import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.INCREMENTAL_TO_AUTO_TAG;
import static org.apache.paimon.CoreOptions.PRIMARY_KEY;
import static org.apache.paimon.CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_MODE;
import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_WATERMARK;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MAX;
import static org.apache.paimon.CoreOptions.SNAPSHOT_NUM_RETAINED_MIN;
import static org.apache.paimon.CoreOptions.STREAMING_READ_OVERWRITE;
import static org.apache.paimon.mergetree.compact.PartialUpdateMergeFunction.SEQUENCE_GROUP;
import static org.apache.paimon.table.SpecialFields.KEY_FIELD_PREFIX;
import static org.apache.paimon.table.SpecialFields.SYSTEM_FIELD_NAMES;
import static org.apache.paimon.types.DataTypeRoot.ARRAY;
import static org.apache.paimon.types.DataTypeRoot.MAP;
import static org.apache.paimon.types.DataTypeRoot.MULTISET;
import static org.apache.paimon.types.DataTypeRoot.ROW;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Validation utils for {@link TableSchema}. */
public class SchemaValidation {

    public static final List<Class<? extends DataType>> PRIMARY_KEY_UNSUPPORTED_LOGICAL_TYPES =
            Arrays.asList(MapType.class, ArrayType.class, RowType.class, MultisetType.class);

    /**
     * Validate the {@link TableSchema} and {@link CoreOptions}.
     *
     * <p>TODO validate all items in schema and all keys in options.
     *
     * @param schema the schema to be validated
     */
    public static void validateTableSchema(TableSchema schema) {
        CoreOptions options = new CoreOptions(schema.options());

        validateOnlyContainPrimitiveType(schema.fields(), schema.primaryKeys(), "primary key");
        validateOnlyContainPrimitiveType(schema.fields(), schema.partitionKeys(), "partition");
        validateOnlyContainPrimitiveType(schema.fields(), options.upsertKey(), "upsert key");

        if (!options.upsertKey().isEmpty() && !schema.primaryKeys().isEmpty()) {
            throw new RuntimeException(
                    String.format(
                            "Cannot define 'upsert-key' %s with 'primary-key' %s.",
                            options.upsertKey(), schema.primaryKeys()));
        }

        validateBucket(schema, options);

        validateStartupMode(options);

        validateFieldsPrefix(schema, options);

        validateSequenceField(schema, options);

        validateSequenceGroup(schema, options);

        ChangelogProducer changelogProducer = options.changelogProducer();
        if (schema.primaryKeys().isEmpty() && changelogProducer != ChangelogProducer.NONE) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Can not set %s on table without primary keys, please define primary keys.",
                            CHANGELOG_PRODUCER.key()));
        }
        if (options.streamingReadOverwrite()
                && (changelogProducer == ChangelogProducer.FULL_COMPACTION
                        || changelogProducer == ChangelogProducer.LOOKUP)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Cannot set %s to true when changelog producer is %s or %s because it will read duplicated changes.",
                            STREAMING_READ_OVERWRITE.key(),
                            ChangelogProducer.FULL_COMPACTION,
                            ChangelogProducer.LOOKUP));
        }

        checkArgument(
                options.snapshotNumRetainMin() > 0,
                SNAPSHOT_NUM_RETAINED_MIN.key() + " should be at least 1");
        checkArgument(
                options.snapshotNumRetainMin() <= options.snapshotNumRetainMax(),
                SNAPSHOT_NUM_RETAINED_MIN.key()
                        + " should not be larger than "
                        + SNAPSHOT_NUM_RETAINED_MAX.key());

        checkArgument(
                options.changelogNumRetainMin() > 0,
                CHANGELOG_NUM_RETAINED_MIN.key() + " should be at least 1");
        checkArgument(
                options.changelogNumRetainMin() <= options.changelogNumRetainMax(),
                CHANGELOG_NUM_RETAINED_MIN.key()
                        + " should not be larger than "
                        + CHANGELOG_NUM_RETAINED_MAX.key());

        FileFormat fileFormat =
                FileFormat.fromIdentifier(options.formatType(), new Options(schema.options()));
        fileFormat.validateDataFields(new RowType(schema.fields()));

        // Check column names in schema
        schema.fieldNames()
                .forEach(
                        f -> {
                            checkState(
                                    !SYSTEM_FIELD_NAMES.contains(f),
                                    String.format(
                                            "Field name[%s] in schema cannot be exist in %s",
                                            f, SYSTEM_FIELD_NAMES));
                            checkState(
                                    !f.startsWith(KEY_FIELD_PREFIX),
                                    String.format(
                                            "Field name[%s] in schema cannot start with [%s]",
                                            f, KEY_FIELD_PREFIX));
                        });

        if (schema.primaryKeys().isEmpty() && options.streamingReadOverwrite()) {
            throw new RuntimeException(
                    String.format(
                            "Doesn't support streaming read the changes from overwrite when the primary keys are "
                                    + "not defined. Please use %s to enable the streaming read overwrite commit for append table.",
                            CoreOptions.STREAMING_READ_APPEND_OVERWRITE.key()));
        }

        if (schema.options().containsKey(CoreOptions.PARTITION_EXPIRATION_TIME.key())) {
            if (schema.partitionKeys().isEmpty()) {
                throw new IllegalArgumentException(
                        "Can not set 'partition.expiration-time' for non-partitioned table.");
            }
        }

        String recordLevelTimeField = options.recordLevelTimeField();
        if (recordLevelTimeField != null) {
            Optional<DataField> field =
                    schema.fields().stream()
                            .filter(dataField -> dataField.name().equals(recordLevelTimeField))
                            .findFirst();
            if (!field.isPresent()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Can not find time field %s for record level expire.",
                                recordLevelTimeField));
            }
            DataType dataType = field.get().type();
            if (!(dataType instanceof IntType
                    || dataType instanceof BigIntType
                    || dataType instanceof TimestampType
                    || dataType instanceof LocalZonedTimestampType)) {
                throw new IllegalArgumentException(
                        String.format(
                                "The record level time field type should be one of INT, BIGINT, or TIMESTAMP, but field type is %s.",
                                dataType));
            }
        }

        if (options.mergeEngine() == MergeEngine.FIRST_ROW) {
            if (options.changelogProducer() != ChangelogProducer.LOOKUP
                    && options.changelogProducer() != ChangelogProducer.NONE) {
                throw new IllegalArgumentException(
                        "Only support 'none' and 'lookup' changelog-producer on FIRST_ROW merge engine");
            }
        }

        options.rowkindField()
                .ifPresent(
                        field ->
                                checkArgument(
                                        schema.fieldNames().contains(field),
                                        "Rowkind field: '%s' can not be found in table schema.",
                                        field));

        if (options.deletionVectorsEnabled()) {
            validateForDeletionVectors(options);
        }

        validateMergeFunctionFactory(schema);

        validateRowLineage(schema, options);
    }

    public static void validateFallbackBranch(SchemaManager schemaManager, TableSchema schema) {
        String fallbackBranch = schema.options().get(CoreOptions.SCAN_FALLBACK_BRANCH.key());
        if (!StringUtils.isNullOrWhitespaceOnly(fallbackBranch)) {
            checkArgument(
                    schemaManager.copyWithBranch(fallbackBranch).latest().isPresent(),
                    "Cannot set '%s' = '%s' because the branch '%s' isn't existed.",
                    CoreOptions.SCAN_FALLBACK_BRANCH.key(),
                    fallbackBranch,
                    fallbackBranch);
        }
    }

    private static void validateOnlyContainPrimitiveType(
            List<DataField> fields, List<String> fieldNames, String errorMessageIntro) {
        if (!fieldNames.isEmpty()) {
            Map<String, DataField> rowFields = new HashMap<>();
            for (DataField rowField : fields) {
                rowFields.put(rowField.name(), rowField);
            }
            for (String fieldName : fieldNames) {
                DataField rowField = rowFields.get(fieldName);
                if (rowField == null) {
                    throw new IllegalArgumentException("Cannot find field: " + fieldName);
                }
                DataType dataType = rowField.type();
                if (PRIMARY_KEY_UNSUPPORTED_LOGICAL_TYPES.stream()
                        .anyMatch(c -> c.isInstance(dataType))) {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "The type %s in %s field %s is unsupported",
                                    dataType.getClass().getSimpleName(),
                                    errorMessageIntro,
                                    fieldName));
                }
            }
        }
    }

    private static void validateStartupMode(CoreOptions options) {
        if (options.startupMode() == CoreOptions.StartupMode.FROM_TIMESTAMP) {
            checkExactOneOptionExistInMode(
                    options, options.startupMode(), SCAN_TIMESTAMP_MILLIS, SCAN_TIMESTAMP);
            checkOptionsConflict(
                    options,
                    Arrays.asList(
                            SCAN_SNAPSHOT_ID,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            SCAN_TAG_NAME,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN,
                            INCREMENTAL_TO_AUTO_TAG),
                    Arrays.asList(SCAN_TIMESTAMP_MILLIS, SCAN_TIMESTAMP));
        } else if (options.startupMode() == CoreOptions.StartupMode.FROM_SNAPSHOT) {
            checkExactOneOptionExistInMode(
                    options,
                    options.startupMode(),
                    SCAN_SNAPSHOT_ID,
                    SCAN_TAG_NAME,
                    SCAN_WATERMARK);
            checkOptionsConflict(
                    options,
                    Arrays.asList(
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_TIMESTAMP,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN,
                            INCREMENTAL_TO_AUTO_TAG),
                    Arrays.asList(SCAN_SNAPSHOT_ID, SCAN_TAG_NAME));
        } else if (options.startupMode() == CoreOptions.StartupMode.INCREMENTAL) {
            checkExactOneOptionExistInMode(
                    options,
                    options.startupMode(),
                    INCREMENTAL_BETWEEN,
                    INCREMENTAL_BETWEEN_TIMESTAMP,
                    INCREMENTAL_TO_AUTO_TAG);
            checkOptionsConflict(
                    options,
                    Arrays.asList(
                            SCAN_SNAPSHOT_ID,
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            SCAN_TIMESTAMP,
                            SCAN_TAG_NAME),
                    Arrays.asList(
                            INCREMENTAL_BETWEEN,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_TO_AUTO_TAG));
        } else if (options.startupMode() == CoreOptions.StartupMode.FROM_SNAPSHOT_FULL) {
            checkOptionExistInMode(options, SCAN_SNAPSHOT_ID, options.startupMode());
            checkOptionsConflict(
                    options,
                    Arrays.asList(
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_TIMESTAMP,
                            SCAN_FILE_CREATION_TIME_MILLIS,
                            SCAN_TAG_NAME,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN,
                            INCREMENTAL_TO_AUTO_TAG),
                    Collections.singletonList(SCAN_SNAPSHOT_ID));
        } else if (options.startupMode() == CoreOptions.StartupMode.FROM_FILE_CREATION_TIME) {
            checkOptionExistInMode(
                    options,
                    SCAN_FILE_CREATION_TIME_MILLIS,
                    CoreOptions.StartupMode.FROM_FILE_CREATION_TIME);
            checkOptionsConflict(
                    options,
                    Arrays.asList(
                            SCAN_SNAPSHOT_ID,
                            SCAN_TIMESTAMP_MILLIS,
                            SCAN_TAG_NAME,
                            INCREMENTAL_BETWEEN_TIMESTAMP,
                            INCREMENTAL_BETWEEN,
                            INCREMENTAL_TO_AUTO_TAG),
                    Collections.singletonList(SCAN_FILE_CREATION_TIME_MILLIS));
        } else {
            checkOptionNotExistInMode(options, SCAN_TIMESTAMP_MILLIS, options.startupMode());
            checkOptionNotExistInMode(
                    options, SCAN_FILE_CREATION_TIME_MILLIS, options.startupMode());
            checkOptionNotExistInMode(options, SCAN_TIMESTAMP, options.startupMode());
            checkOptionNotExistInMode(options, SCAN_SNAPSHOT_ID, options.startupMode());
            checkOptionNotExistInMode(options, SCAN_TAG_NAME, options.startupMode());
            checkOptionNotExistInMode(
                    options, INCREMENTAL_BETWEEN_TIMESTAMP, options.startupMode());
            checkOptionNotExistInMode(options, INCREMENTAL_BETWEEN, options.startupMode());
            checkOptionNotExistInMode(options, INCREMENTAL_TO_AUTO_TAG, options.startupMode());
        }
    }

    private static void checkOptionExistInMode(
            CoreOptions options, ConfigOption<?> option, CoreOptions.StartupMode startupMode) {
        checkArgument(
                options.toConfiguration().contains(option),
                String.format(
                        "%s can not be null when you use %s for %s",
                        option.key(), startupMode, SCAN_MODE.key()));
    }

    private static void checkOptionNotExistInMode(
            CoreOptions options, ConfigOption<?> option, CoreOptions.StartupMode startupMode) {
        checkArgument(
                !options.toConfiguration().contains(option),
                String.format(
                        "%s must be null when you use %s for %s",
                        option.key(), startupMode, SCAN_MODE.key()));
    }

    private static void checkExactOneOptionExistInMode(
            CoreOptions options,
            CoreOptions.StartupMode startupMode,
            ConfigOption<?>... configOptions) {
        checkArgument(
                Arrays.stream(configOptions)
                                .filter(op -> options.toConfiguration().contains(op))
                                .count()
                        == 1,
                String.format(
                        "must set only one key in [%s] when you use %s for %s",
                        concatConfigKeys(Arrays.asList(configOptions)),
                        startupMode,
                        SCAN_MODE.key()));
    }

    private static void checkOptionsConflict(
            CoreOptions options,
            List<ConfigOption<?>> illegalOptions,
            List<ConfigOption<?>> legalOptions) {
        for (ConfigOption<?> illegalOption : illegalOptions) {
            checkArgument(
                    !options.toConfiguration().contains(illegalOption),
                    "[%s] must be null when you set [%s]",
                    illegalOption.key(),
                    concatConfigKeys(legalOptions));
        }
    }

    private static String concatConfigKeys(List<ConfigOption<?>> configOptions) {
        return configOptions.stream().map(ConfigOption::key).collect(Collectors.joining(","));
    }

    private static void validateFieldsPrefix(TableSchema schema, CoreOptions options) {
        List<String> fieldNames = schema.fieldNames();
        options.toMap()
                .keySet()
                .forEach(
                        k -> {
                            if (k.startsWith(FIELDS_PREFIX)) {
                                String[] fields = k.split("\\.")[1].split(FIELDS_SEPARATOR);
                                for (String field : fields) {
                                    checkArgument(
                                            DEFAULT_AGG_FUNCTION.equals(field)
                                                    || fieldNames.contains(field),
                                            String.format(
                                                    "Field %s can not be found in table schema.",
                                                    field));
                                }
                            }
                        });
    }

    private static void validateSequenceGroup(TableSchema schema, CoreOptions options) {
        Map<String, Set<String>> fields2Group = new HashMap<>();
        for (Map.Entry<String, String> entry : options.toMap().entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            List<String> fieldNames = schema.fieldNames();
            if (k.startsWith(FIELDS_PREFIX) && k.endsWith(SEQUENCE_GROUP)) {
                String[] sequenceFieldNames =
                        k.substring(
                                        FIELDS_PREFIX.length() + 1,
                                        k.length() - SEQUENCE_GROUP.length() - 1)
                                .split(FIELDS_SEPARATOR);

                for (String field : v.split(FIELDS_SEPARATOR)) {
                    if (!fieldNames.contains(field)) {
                        throw new IllegalArgumentException(
                                String.format("Field %s can not be found in table schema.", field));
                    }

                    List<String> sequenceFieldsList = new ArrayList<>();
                    for (String sequenceFieldName : sequenceFieldNames) {
                        if (!fieldNames.contains(sequenceFieldName)) {
                            throw new IllegalArgumentException(
                                    String.format(
                                            "The sequence field group: %s can not be found in table schema.",
                                            sequenceFieldName));
                        }
                        sequenceFieldsList.add(sequenceFieldName);
                    }

                    if (fields2Group.containsKey(field)) {
                        List<List<String>> sequenceGroups = new ArrayList<>();
                        sequenceGroups.add(new ArrayList<>(fields2Group.get(field)));
                        sequenceGroups.add(sequenceFieldsList);

                        throw new IllegalArgumentException(
                                String.format(
                                        "Field %s is defined repeatedly by multiple groups: %s.",
                                        field, sequenceGroups));
                    }

                    Set<String> group = fields2Group.computeIfAbsent(field, p -> new HashSet<>());
                    group.addAll(sequenceFieldsList);
                }
            }
        }
        Set<String> illegalGroup =
                fields2Group.values().stream()
                        .flatMap(Collection::stream)
                        .filter(g -> options.fieldAggFunc(g) != null)
                        .collect(Collectors.toSet());
        if (!illegalGroup.isEmpty()) {
            throw new IllegalArgumentException(
                    "Should not defined aggregation function on sequence group: " + illegalGroup);
        }
    }

    private static void validateForDeletionVectors(CoreOptions options) {
        checkArgument(
                options.changelogProducer() == ChangelogProducer.NONE
                        || options.changelogProducer() == ChangelogProducer.INPUT
                        || options.changelogProducer() == ChangelogProducer.LOOKUP,
                "Deletion vectors mode is only supported for NONE/INPUT/LOOKUP changelog producer now.");

        checkArgument(
                !options.mergeEngine().equals(MergeEngine.FIRST_ROW),
                "First row merge engine does not need deletion vectors because there is no deletion of old data in this merge engine.");
    }

    private static void validateSequenceField(TableSchema schema, CoreOptions options) {
        List<String> sequenceField = options.sequenceField();
        if (!sequenceField.isEmpty()) {
            Map<String, Integer> fieldCount =
                    sequenceField.stream()
                            .collect(Collectors.toMap(field -> field, field -> 1, Integer::sum));

            sequenceField.forEach(
                    field -> {
                        checkArgument(
                                schema.fieldNames().contains(field),
                                "Sequence field: '%s' can not be found in table schema.",
                                field);

                        checkArgument(
                                options.fieldAggFunc(field) == null,
                                "Should not define aggregation on sequence field: '%s'.",
                                field);

                        checkArgument(
                                fieldCount.get(field) == 1,
                                "Sequence field '%s' is defined repeatedly.",
                                field);
                    });

            if (options.mergeEngine() == MergeEngine.FIRST_ROW) {
                throw new IllegalArgumentException(
                        "Do not support use sequence field on FIRST_ROW merge engine.");
            }

            if (schema.crossPartitionUpdate()) {
                throw new IllegalArgumentException(
                        String.format(
                                "You can not use sequence.field in cross partition update case "
                                        + "(Primary key constraint '%s' not include all partition fields '%s').",
                                schema.primaryKeys(), schema.partitionKeys()));
            }
        }
    }

    private static void validateBucket(TableSchema schema, CoreOptions options) {
        int bucket = options.bucket();
        if (bucket == -1) {
            if (options.toMap().get(BUCKET_KEY.key()) != null) {
                throw new RuntimeException(
                        "Cannot define 'bucket-key' with bucket = -1, please remove the 'bucket-key' setting or specify a bucket number.");
            }

            if (schema.primaryKeys().isEmpty()
                    && options.toMap().get(FULL_COMPACTION_DELTA_COMMITS.key()) != null) {
                throw new RuntimeException(
                        "AppendOnlyTable of unaware or dynamic bucket does not support 'full-compaction.delta-commits'");
            }
        } else if (bucket < 1 && !isPostponeBucketTable(schema, bucket)) {
            throw new RuntimeException("The number of buckets needs to be greater than 0.");
        } else {
            if (schema.crossPartitionUpdate()) {
                throw new IllegalArgumentException(
                        String.format(
                                "You should use dynamic bucket (bucket = -1) mode in cross partition update case "
                                        + "(Primary key constraint %s not include all partition fields %s).",
                                schema.primaryKeys(), schema.partitionKeys()));
            }

            if (schema.primaryKeys().isEmpty() && schema.bucketKeys().isEmpty()) {
                throw new RuntimeException(
                        "You should define a 'bucket-key' for bucketed append mode.");
            }

            if (!schema.bucketKeys().isEmpty()) {
                List<String> bucketKeys = schema.bucketKeys();
                List<String> nestedFields =
                        schema.fields().stream()
                                .filter(
                                        dataField ->
                                                bucketKeys.contains(dataField.name())
                                                        && (dataField.type().getTypeRoot() == ARRAY
                                                                || dataField.type().getTypeRoot()
                                                                        == MULTISET
                                                                || dataField.type().getTypeRoot()
                                                                        == MAP
                                                                || dataField.type().getTypeRoot()
                                                                        == ROW))
                                .map(DataField::name)
                                .collect(Collectors.toList());
                if (!nestedFields.isEmpty()) {
                    throw new RuntimeException(
                            "nested type can not in bucket-key, in your table these key are "
                                    + nestedFields);
                }
            }
        }
    }

    private static boolean isPostponeBucketTable(TableSchema schema, int bucket) {
        return !schema.primaryKeys().isEmpty() && bucket == BucketMode.POSTPONE_BUCKET;
    }

    private static void validateMergeFunctionFactory(TableSchema schema) {
        if (schema.primaryKeys().isEmpty()) {
            return;
        }
        CoreOptions options = new CoreOptions(schema.options());
        switch (options.mergeEngine()) {
            case DEDUPLICATE:
            case FIRST_ROW:
                return;
            default:
        }

        for (int i = 0; i < schema.logicalRowType().getFieldNames().size(); i++) {
            String fieldName = schema.logicalRowType().getFieldNames().get(i);
            String aggFuncName = options.fieldAggFunc(fieldName);
            aggFuncName = aggFuncName == null ? options.fieldsDefaultFunc() : aggFuncName;
            if (aggFuncName != null) {
                FactoryUtil.discoverFactory(
                        FieldAggregator.class.getClassLoader(),
                        FieldAggregatorFactory.class,
                        aggFuncName);
            }
        }
    }

    private static void validateRowLineage(TableSchema schema, CoreOptions options) {
        if (options.rowTrackingEnabled()) {
            checkArgument(
                    options.bucket() == -1,
                    "Cannot define %s for row lineage table, it only support bucket = -1",
                    CoreOptions.BUCKET.key());
            checkArgument(
                    schema.primaryKeys().isEmpty(),
                    "Cannot define %s for row lineage table.",
                    PRIMARY_KEY.key());
        }
    }
}
