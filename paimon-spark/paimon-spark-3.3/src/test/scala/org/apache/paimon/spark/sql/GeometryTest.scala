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

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.SparkCatalog
import org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.SedonaSqlExtensions
import org.apache.spark.SparkConf
import org.apache.spark.paimon.Utils
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.jupiter.api.Assertions

import java.io.File

class GeometryTest extends QueryTest with SharedSparkSession {
  protected lazy val tempDBDir: File = Utils.createTempDir

  protected val fileFormats: Seq[String] = Seq("orc", "parquet", "avro")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.paimon", classOf[SparkCatalog].getName)
      .set("spark.sql.catalog.paimon.warehouse", tempDBDir.getCanonicalPath)
      .set(
        "spark.sql.extensions",
        classOf[PaimonSparkSessionExtensions].getName + "," + classOf[SedonaSqlExtensions].getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"USE paimon")
  }

  test("DDL: create table with geometry") {
    fileFormats.foreach {
      format =>
        withTable("t", "tt") {
          // CREATE TABLE
          sql(s"""
                 |CREATE TABLE t (id INT NOT NULL, geom GEOMETRY NOT NULL, h3 INT)
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |""".stripMargin)
          val schema = spark.table("t").schema
          Assertions.assertEquals(schema.size, 3)
          Assertions.assertFalse(schema("id").nullable)
          Assertions.assertTrue(schema("geom").nullable)

          // CREATE TABLE AS
          sql(s"""
                 |CREATE TABLE tt
                 |PARTITIONED BY (h3)
                 |TBLPROPERTIES ( 'file.format'='$format' )
                 |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom, 10 as h3
                 |""".stripMargin)
          Assertions.assertEquals(spark.table("tt").schema.size, 3)
        }
    }
  }

  test("DDL: create table with geometry, select") {
    fileFormats.foreach {
      format =>
        withTable("t", "tt") {
          sql(s"""
                 |CREATE TABLE t (id INT, geom GEOMETRY, h3 INT)
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |""".stripMargin)
          select("t")

          sql(s"""
                 |CREATE TABLE tt (id INT, geom GEOMETRY, h3 INT)
                 |PARTITIONED BY (h3)
                 |TBLPROPERTIES ( 'file.format'='$format' )
                 |""".stripMargin)
          select("tt")
        }
    }
  }

  test("DDL: create table with geometry, simple dml") {
    fileFormats.foreach {
      format =>
        withTable("t", "tt") {
          sql(s"""
                 |CREATE TABLE t
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom, 1 as h3
                 |""".stripMargin)
          dml("t")

          sql(s"""
                 |CREATE TABLE tt
                 |PARTITIONED BY (h3)
                 |TBLPROPERTIES ( 'file.format'='$format' )
                 |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom, 1 as h3
                 |""".stripMargin)
          dml("tt")

          // Merge
          sql(s"""
                 |MERGE INTO t
                 |USING tt
                 |ON t.id = tt.id
                 |WHEN MATCHED THEN
                 |UPDATE SET geom = ST_GeomFromText('POINT(3 3 3)')
                 |""".stripMargin)
          sql(s"""
                 |MERGE INTO t
                 |USING tt
                 |ON t.id = tt.h3
                 |WHEN NOT MATCHED THEN
                 |INSERT (id, geom, h3) values (tt.id+1, tt.geom, tt.h3-1)
                 |""".stripMargin)
          Assertions.assertEquals(spark.sql(s"select * from t").count(), 2)
        }
    }
  }

  test("DDL: create table with geometry, merge") {
    fileFormats.foreach {
      format =>
        withTable("t", "tt") {
          sql(s"""
                 |CREATE TABLE t
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom, 1 as h3
                 |""".stripMargin)
          sql(s"""
                 |INSERT INTO t VALUES
                 |( 2, ST_GeomFromText('POINT(0 1)'), 2 ),
                 |( 3, ST_GeomFromText('POINT(1 1)'), 3 )
                 |""".stripMargin)

          sql(s"""
                 |CREATE TABLE tt
                 |PARTITIONED BY (h3)
                 |TBLPROPERTIES ( 'file.format'='$format' )
                 |AS SELECT 1 as id, ST_GeomFromText('POINT(3 2 1)') as geom, 1 as h3
                 |""".stripMargin)
          sql(s"""
                 |INSERT INTO tt VALUES
                 |( 2, ST_GeomFromText('POINT(2 1)'), 3 ),
                 |( 3, ST_GeomFromText('POINT(3 1)'), 4 ),
                 |( 4, ST_GeomFromText('POINT(4 2)'), 2 ),
                 |( 5, ST_GeomFromText('POINT(3 3 1)'), 3 )
                 |""".stripMargin)

          // Merge
          sql(s"""
                 |MERGE INTO t
                 |USING tt
                 |ON t.id = tt.id
                 |WHEN MATCHED THEN
                 |  UPDATE SET geom = tt.geom
                 |WHEN NOT MATCHED THEN
                 |  INSERT (id, geom, h3) values (tt.id, tt.geom, tt.h3)
                 |""".stripMargin)
          Assertions.assertEquals(spark.sql(s"select * from t").count(), 5)
        }
    }
  }

  private def select(tab: String) = {
    sql(s"""
           |INSERT INTO $tab VALUES
           |( 1, ST_GeomFromText('POINT(0 1)'), 1 ),
           |( 2, ST_GeomFromText('POINT(0 1)'), 2 ),
           |( 3, ST_GeomFromText('POINT(1 1)'), 3 ),
           |( 4, ST_GeomFromText('POINT(2 2)'), 4 ),
           |( 5, ST_GeomFromText('POINT(3 3 1)'), 5 )
           |""".stripMargin)

    val count = sql(s"""
                       |SELECT * FROM $tab
                       |WHERE ST_Intersects( geom, ST_GeomFromText('POINT(1 1)') )
                       |""".stripMargin).count()
    Assertions.assertEquals(count, 1)

    val count1 = sql(s"""
                        |SELECT * FROM $tab
                        |WHERE ST_Contains( ST_GeomFromText('LINESTRING(0 0, 3 3)'), geom )
                        |""".stripMargin).count()
    Assertions.assertEquals(count1, 2)
  }

  private def dml(tab: String) = {
    // INSERT
    sql(s"""
           |INSERT INTO $tab VALUES
           |( 2, ST_GeomFromText('POINT(0 0)'), 2 ),
           |( 3, ST_GeomFromText('POINT(1 1)'), 3 )
           |""".stripMargin)
    sql(s"""
           |INSERT INTO $tab
           |SELECT 4, ST_GeomFromText('POINT(2 2)'), 4
           |""".stripMargin)
    val count1 = spark.sql(s"select * from $tab").count()

    // INSERT OVERWRITE
    sql(s"""
           |INSERT OVERWRITE $tab
           |SELECT 0, ST_GeomFromText('POINT(3 2 1)'), 0
           |""".stripMargin)
    val count2 = spark.sql(s"select * from $tab").count()
    Assertions.assertTrue(count2 < count1)

    // DELETE
    sql(s"""
           |DELETE FROM $tab
           |WHERE ST_Intersects( geom, ST_GeomFromText('POINT(3 2 1)') )
           |""".stripMargin)
    Assertions.assertEquals(spark.sql(s"select * from $tab").count(), 0)

    // UPDATE
    sql(s"""
           |INSERT OVERWRITE $tab
           |SELECT 0, ST_GeomFromText('POINT(3 2 1)'), 1
           |""".stripMargin)
    val col1 = spark.sql(s"select geom from $tab").collect()
    sql(s"""
           |UPDATE $tab
           |SET geom = ST_GeomFromText('POINT(1 2 3)')
           |""".stripMargin)
    val col2 = spark.sql(s"select geom from $tab").collect()
    Assertions.assertEquals(col1(0).get(0).toString, "POINT (3 2)")
    Assertions.assertEquals(col2(0).get(0).toString, "POINT (1 2)")
  }

}
