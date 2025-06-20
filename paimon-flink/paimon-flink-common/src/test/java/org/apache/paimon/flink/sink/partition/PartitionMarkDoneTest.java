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

package org.apache.paimon.flink.sink.partition;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFileTestUtils;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.io.IndexIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.TableTestBase;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.paimon.CoreOptions.DELETION_VECTORS_ENABLED;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_ACTION;
import static org.apache.paimon.CoreOptions.PARTITION_MARK_DONE_WHEN_END_INPUT;
import static org.assertj.core.api.Assertions.assertThat;

class PartitionMarkDoneTest extends TableTestBase {

    @Test
    public void testTriggerByCompaction() throws Exception {
        innerTest(true, true);
    }

    @Test
    public void testNotTriggerByCompaction() throws Exception {
        innerTest(false, true);
    }

    @Test
    public void testNotTriggerWhenRecoveryFromState() throws Exception {
        innerTest(false, false);
    }

    private void innerTest(boolean deletionVectors, boolean partitionMarkDoneRecoverFromState)
            throws Exception {
        Identifier identifier = identifier("T");
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .partitionKeys("a")
                        .primaryKey("a", "b")
                        .option(PARTITION_MARK_DONE_WHEN_END_INPUT.key(), "true")
                        .option(PARTITION_MARK_DONE_ACTION.key(), "success-file")
                        .option(
                                DELETION_VECTORS_ENABLED.key(),
                                Boolean.valueOf(deletionVectors).toString())
                        .build();
        catalog.createTable(identifier, schema, true);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        Path location = table.location();
        Path successFile = new Path(location, "a=0/_SUCCESS");
        PartitionMarkDoneListener markDone =
                PartitionMarkDoneListener.create(
                                getClass().getClassLoader(),
                                false,
                                false,
                                new MockOperatorStateStore(),
                                table)
                        .get();

        if (!partitionMarkDoneRecoverFromState) {
            notifyCommits(markDone, false, partitionMarkDoneRecoverFromState);
            assertThat(table.fileIO().exists(successFile)).isEqualTo(false);
            return;
        }

        notifyCommits(markDone, true, partitionMarkDoneRecoverFromState);
        assertThat(table.fileIO().exists(successFile)).isEqualTo(deletionVectors);

        if (!deletionVectors) {
            notifyCommits(markDone, false, partitionMarkDoneRecoverFromState);
            assertThat(table.fileIO().exists(successFile)).isEqualTo(true);
        }
    }

    public static void notifyCommits(PartitionMarkDoneListener markDone, boolean isCompact) {
        notifyCommits(markDone, isCompact, true);
    }

    private static void notifyCommits(
            PartitionMarkDoneListener markDone,
            boolean isCompact,
            boolean partitionMarkDoneRecoverFromState) {
        ManifestCommittable committable = new ManifestCommittable(Long.MAX_VALUE);
        DataFileMeta file = DataFileTestUtils.newFile();
        CommitMessageImpl compactMessage;
        if (isCompact) {
            compactMessage =
                    new CommitMessageImpl(
                            BinaryRow.singleColumn(0),
                            0,
                            1,
                            new DataIncrement(emptyList(), emptyList(), emptyList()),
                            new CompactIncrement(singletonList(file), emptyList(), emptyList()),
                            new IndexIncrement(emptyList()));
        } else {
            compactMessage =
                    new CommitMessageImpl(
                            BinaryRow.singleColumn(0),
                            0,
                            1,
                            new DataIncrement(singletonList(file), emptyList(), emptyList()),
                            new CompactIncrement(emptyList(), emptyList(), emptyList()),
                            new IndexIncrement(emptyList()));
        }
        committable.addFileCommittable(compactMessage);
        if (partitionMarkDoneRecoverFromState) {
            markDone.notifyCommittable(singletonList(committable));
        }
    }

    public static class MockOperatorStateStore implements OperatorStateStore {

        @Override
        public <K, V> BroadcastState<K, V> getBroadcastState(
                MapStateDescriptor<K, V> stateDescriptor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) {
            return new MockListState<>();
        }

        @Override
        public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) {
            throw new UnsupportedOperationException();
        }

        // @Override is skipped for compatibility with Flink 1.x.
        public <K, V> BroadcastState<K, V> getBroadcastState(
                org.apache.flink.api.common.state.v2.MapStateDescriptor<K, V> mapStateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        // @Override is skipped for compatibility with Flink 1.x.
        public <S> org.apache.flink.api.common.state.v2.ListState<S> getListState(
                org.apache.flink.api.common.state.v2.ListStateDescriptor<S> listStateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        // @Override is skipped for compatibility with Flink 1.x.
        public <S> org.apache.flink.api.common.state.v2.ListState<S> getUnionListState(
                org.apache.flink.api.common.state.v2.ListStateDescriptor<S> listStateDescriptor)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getRegisteredStateNames() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> getRegisteredBroadcastStateNames() {
            throw new UnsupportedOperationException();
        }
    }

    public static class MockListState<T> implements ListState<T> {

        private final List<T> backingList = new ArrayList<>();

        public MockListState() {}

        @Override
        public void update(List<T> values) {
            this.backingList.clear();
            this.addAll(values);
        }

        @Override
        public void addAll(List<T> values) {
            this.backingList.addAll(values);
        }

        @Override
        public Iterable<T> get() {
            return new Iterable<T>() {
                @Nonnull
                public Iterator<T> iterator() {
                    return MockListState.this.backingList.iterator();
                }
            };
        }

        @Override
        public void add(T value) {
            this.backingList.add(value);
        }

        @Override
        public void clear() {
            this.backingList.clear();
        }
    }
}
