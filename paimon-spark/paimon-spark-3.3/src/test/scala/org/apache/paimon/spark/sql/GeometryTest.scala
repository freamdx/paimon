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

class GeometryTest extends QueryTest with SharedSparkSession with WithTableOptions {
  protected lazy val tempDBDir: File = Utils.createTempDir

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
        withTable("T", "TT") {
          // CREATE TABLE
          sql(s"""
                 |CREATE TABLE T (id INT NOT NULL, bi BIGINT, geom GEOMETRY NOT NULL)
                 |PARTITIONED BY (bi)
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |""".stripMargin)
          val schema = spark.table("T").schema
          Assertions.assertEquals(schema.size, 3)
          Assertions.assertFalse(schema("id").nullable)
          Assertions.assertTrue(schema("geom").nullable)

          // CREATE TABLE AS
          sql(s"""
                 |CREATE TABLE TT
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |AS SELECT 1 as id, 10 as bi, ST_GeomFromText('POINT(1 2 3)') as geom
                 |""".stripMargin)
          Assertions.assertEquals(spark.table("TT").schema.size, 3)
        }
    }
  }

  test("DDL: create table with geometry, select") {
    fileFormats.foreach {
      format =>
        withTable("T") {
          sql(s"""
                 |CREATE TABLE T
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom
                 |""".stripMargin)

          sql("""
                |INSERT INTO T VALUES
                |( 2, ST_GeomFromText('POINT(0 1)') ),
                |( 3, ST_GeomFromText('POINT(1 1)') ),
                |( 4, ST_GeomFromText('POINT(2 2)') ),
                |( 5, ST_GeomFromText('POINT(3 3 1)') )
                |""".stripMargin)

          val count = sql("""
                            |SELECT * FROM T
                            |WHERE ST_Intersects( geom, ST_GeomFromText('POINT(1 1)') )
                            |""".stripMargin).count()
          Assertions.assertEquals(count, 1)

          val count1 = sql("""
                             |SELECT * FROM T
                             |WHERE ST_Contains( ST_GeomFromText('LINESTRING(0 0, 3 3)'), geom )
                             |""".stripMargin).count()
          Assertions.assertEquals(count1, 2)
        }
    }
  }

  test("DDL: create table with geometry, simple dml") {
    fileFormats.foreach {
      format =>
        withTable("T") {
          sql(s"""
                 |CREATE TABLE T
                 |TBLPROPERTIES ( 'primary-key'='id', 'file.format'='$format' )
                 |AS SELECT 1 as id, ST_GeomFromText('POINT(1 2 3)') as geom
                 |""".stripMargin)

          // INSERT
          sql("""
                |INSERT INTO T VALUES
                |( 2, ST_GeomFromText('POINT(0 0)') ),
                |( 3, ST_GeomFromText('POINT(1 1)') )
                |""".stripMargin)
          sql("""
                |INSERT INTO T
                |SELECT 4, ST_GeomFromText('POINT(2 2)')
                |""".stripMargin)
          val count1 = spark.sql("select * from T").count()

          // INSERT OVERWRITE
          sql("""
                |INSERT OVERWRITE T
                |SELECT 0, ST_GeomFromText('POINT(3 2 1)')
                |""".stripMargin)
          val count2 = spark.sql("select * from T").count()
          Assertions.assertTrue(count2 < count1)

          // DELETE
          sql("""
                |DELETE FROM T
                |WHERE ST_Intersects( geom, ST_GeomFromText('POINT(3 2 1)') )
                |""".stripMargin)
          Assertions.assertEquals(spark.sql("select * from T").count(), 0)

          // UPDATE
          sql("""
                |INSERT OVERWRITE T
                |SELECT 0, ST_GeomFromText('POINT(3 2 1)')
                |""".stripMargin)
          val col1 = spark.sql("select geom from T").collect()
          sql("""
                |UPDATE T
                |SET geom = ST_GeomFromText('POINT(1 2 3)')
                |""".stripMargin)
          val col2 = spark.sql("select geom from T").collect()
          Assertions.assertEquals(col1(0).get(0).toString, "POINT (3 2)")
          Assertions.assertEquals(col2(0).get(0).toString, "POINT (1 2)")
        }
    }
  }

}
