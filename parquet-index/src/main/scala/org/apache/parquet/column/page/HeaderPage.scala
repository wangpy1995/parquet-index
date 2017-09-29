package org.apache.parquet.column.page

class HeaderPage(compressedSize:Int,uncompressedSize:Int,offsetIndex:Array[Int])
  extends Page(compressedSize,uncompressedSize){
  
}
