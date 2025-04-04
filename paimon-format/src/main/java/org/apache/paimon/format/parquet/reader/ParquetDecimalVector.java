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

package org.apache.paimon.format.parquet.reader;

import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.columnar.BytesColumnVector;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.DecimalColumnVector;
import org.apache.paimon.data.columnar.Dictionary;
import org.apache.paimon.data.columnar.IntColumnVector;
import org.apache.paimon.data.columnar.LongColumnVector;
import org.apache.paimon.data.columnar.heap.ElementCountable;
import org.apache.paimon.data.columnar.writable.WritableBytesVector;
import org.apache.paimon.data.columnar.writable.WritableColumnVector;
import org.apache.paimon.data.columnar.writable.WritableIntVector;
import org.apache.paimon.data.columnar.writable.WritableLongVector;
import org.apache.paimon.format.parquet.ParquetSchemaConverter;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * Parquet write decimal as int32 and int64 and binary, this class wrap the real vector to provide
 * {@link DecimalColumnVector} interface.
 */
public class ParquetDecimalVector
        implements DecimalColumnVector,
                WritableLongVector,
                WritableIntVector,
                WritableBytesVector,
                ElementCountable {

    private final WritableColumnVector vector;

    public ParquetDecimalVector(WritableColumnVector vector) {
        this.vector = vector;
    }

    @Override
    public Decimal getDecimal(int i, int precision, int scale) {
        if (ParquetSchemaConverter.is32BitDecimal(precision) && vector instanceof IntColumnVector) {
            return Decimal.fromUnscaledLong(((IntColumnVector) vector).getInt(i), precision, scale);
        } else if (ParquetSchemaConverter.is64BitDecimal(precision)
                && vector instanceof LongColumnVector) {
            return Decimal.fromUnscaledLong(
                    ((LongColumnVector) vector).getLong(i), precision, scale);
        } else {
            checkArgument(
                    vector instanceof BytesColumnVector,
                    "Reading decimal type occur unsupported vector type: %s",
                    vector.getClass());
            return Decimal.fromUnscaledBytes(
                    ((BytesColumnVector) vector).getBytes(i).getBytes(), precision, scale);
        }
    }

    public ColumnVector getVector() {
        return vector;
    }

    @Override
    public boolean isNullAt(int i) {
        return vector.isNullAt(i);
    }

    @Override
    public int getCapacity() {
        return vector.getCapacity();
    }

    @Override
    public void reset() {
        vector.reset();
    }

    @Override
    public void setNullAt(int rowId) {
        vector.setNullAt(rowId);
    }

    @Override
    public void setNulls(int rowId, int count) {
        vector.setNulls(rowId, count);
    }

    @Override
    public void fillWithNulls() {
        vector.fillWithNulls();
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        vector.setDictionary(dictionary);
    }

    @Override
    public boolean hasDictionary() {
        return vector.hasDictionary();
    }

    @Override
    public WritableIntVector reserveDictionaryIds(int capacity) {
        return vector.reserveDictionaryIds(capacity);
    }

    @Override
    public WritableIntVector getDictionaryIds() {
        return vector.getDictionaryIds();
    }

    @Override
    public void setAllNull() {
        vector.setAllNull();
    }

    @Override
    public boolean isAllNull() {
        return vector.isAllNull();
    }

    @Override
    public void reserve(int capacity) {
        vector.reserve(capacity);
    }

    @Override
    public int getElementsAppended() {
        return vector.getElementsAppended();
    }

    @Override
    public void addElementsAppended(int num) {
        vector.addElementsAppended(num);
    }

    @Override
    public Bytes getBytes(int i) {
        if (vector instanceof WritableBytesVector) {
            return ((WritableBytesVector) vector).getBytes(i);
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public void putByteArray(int rowId, byte[] value, int offset, int length) {
        if (vector instanceof WritableBytesVector) {
            ((WritableBytesVector) vector).putByteArray(rowId, value, offset, length);
        }
    }

    @Override
    public void fill(byte[] value) {
        if (vector instanceof WritableBytesVector) {
            ((WritableBytesVector) vector).fill(value);
        }
    }

    @Override
    public int getInt(int i) {
        if (vector instanceof WritableIntVector) {
            return ((WritableIntVector) vector).getInt(i);
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public void setInt(int rowId, int value) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setInt(rowId, value);
        }
    }

    @Override
    public void setIntsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setIntsFromBinary(rowId, count, src, srcIndex);
        }
    }

    @Override
    public void setInts(int rowId, int count, int value) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setInts(rowId, count, value);
        }
    }

    @Override
    public void setInts(int rowId, int count, int[] src, int srcIndex) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).setInts(rowId, count, src, srcIndex);
        }
    }

    @Override
    public void fill(int value) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).fill(value);
        }
    }

    @Override
    public void appendInt(int v) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).appendInt(v);
        }
    }

    @Override
    public void appendInts(int count, int v) {
        if (vector instanceof WritableIntVector) {
            ((WritableIntVector) vector).appendInts(count, v);
        }
    }

    @Override
    public long getLong(int i) {
        if (vector instanceof WritableLongVector) {
            return ((WritableLongVector) vector).getLong(i);
        }
        throw new RuntimeException("Child vector must be instance of WritableColumnVector");
    }

    @Override
    public void setLong(int rowId, long value) {
        if (vector instanceof WritableLongVector) {
            ((WritableLongVector) vector).setLong(rowId, value);
        }
    }

    @Override
    public void setLongsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
        if (vector instanceof WritableLongVector) {
            ((WritableLongVector) vector).setLongsFromBinary(rowId, count, src, srcIndex);
        }
    }

    @Override
    public void fill(long value) {
        if (vector instanceof WritableLongVector) {
            ((WritableLongVector) vector).fill(value);
        }
    }
}
