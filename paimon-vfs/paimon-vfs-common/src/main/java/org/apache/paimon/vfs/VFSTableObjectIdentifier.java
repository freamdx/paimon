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

package org.apache.paimon.vfs;

import org.apache.paimon.fs.Path;

import javax.annotation.Nullable;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Identifier for objects under a table. */
public class VFSTableObjectIdentifier extends VFSTableIdentifier {

    private final String relativePath;

    public VFSTableObjectIdentifier(String databaseName, String tableName, String relativePath) {
        this(databaseName, tableName, relativePath, null);
    }

    public VFSTableObjectIdentifier(
            String databaseName,
            String tableName,
            String relativePath,
            @Nullable VFSTableInfo tableInfo) {
        super(databaseName, tableName, tableInfo);
        this.relativePath = relativePath;
    }

    public String relativePath() {
        return relativePath;
    }

    @Override
    public Path filePath() {
        checkNotNull(tableInfo);
        return new Path(tableInfo.tablePath(), relativePath);
    }
}
