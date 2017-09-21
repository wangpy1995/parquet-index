package org.apache.spark.sql.test.parquet.write

import java.io.InputStream

import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.ParquetMetadata

class ParquetConverter extends ParquetMetadataConverter {
  override def readParquetMetadata(from: InputStream, filter: ParquetMetadataConverter.MetadataFilter): ParquetMetadata = {
    val parquetMetadata = super.readParquetMetadata(from, filter)
    import collection.JavaConverters._
    parquetMetadata
  }
}
