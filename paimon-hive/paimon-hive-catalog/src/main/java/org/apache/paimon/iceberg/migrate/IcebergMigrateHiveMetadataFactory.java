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

package org.apache.paimon.iceberg.migrate;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.iceberg.IcebergOptions;
import org.apache.paimon.options.Options;

/** Factory to create {@link IcebergMigrateHiveMetadata}. */
public class IcebergMigrateHiveMetadataFactory implements IcebergMigrateMetadataFactory {
    @Override
    public String identifier() {
        return IcebergOptions.StorageType.HIVE_CATALOG.toString() + "_migrate";
    }

    @Override
    public IcebergMigrateHiveMetadata create(Identifier icebergIdentifier, Options icebergOptions) {
        return new IcebergMigrateHiveMetadata(icebergIdentifier, icebergOptions);
    }
}
