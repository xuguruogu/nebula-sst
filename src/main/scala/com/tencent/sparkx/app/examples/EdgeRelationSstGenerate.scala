package com.tencent.sparkx.app.examples

import com.tencent.sparkx.graphdb.{SSTFileOutputFormat, SortByKeyPartitioner}
import org.apache.spark.sql.SparkSession
import com.tencent.sparkx.graphdb.{SstPartConfig, SstUtils}

/**
 * generate sst files.
 * which can be put into store engine directly.
 */
object EdgeRelationSstGenerate {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.rdd.compress", "true")
      .config("spark.executor.heartbeatInterval", "30s")
      .config("spark.locality.wait", "30s")
      .config("spark.speculation", "false")
      .appName("sst generate")
      .getOrCreate()

    import SstUtils.graphKeyEncodedOrdering
    implicit val sstPartConfig: SstPartConfig = SstPartConfig(100, 1)

    // input format: srcId,dstId,name,cnt
    // output dir:
    // -- 1/1-1.sst
    //     /1-2.sst
    //     /1-3.sst
    // -- 2/2-1.sst
    //     /2-2.sst
    val inputPath: String = "hdsf://"
    val outputPath: String = "hdsf://"
    val propsDef = Array[String]("string", "int")
    val partitions = 100
    val edgeType = 1
    val edgeRank = 0
    val edgeVersion = 0

    spark.sparkContext
      .textFile(inputPath)
      .flatMap({
        line =>
          val cols = line.split(',')
          val srcId = cols(0).toLong
          val dstId = cols(1).toLong

          val key = SstUtils.createEdgeKey(
            SstUtils.getPartition(srcId, partitions),
            srcId,
            edgeType,
            edgeRank,
            dstId,
            edgeVersion
          )

          val keyRev = SstUtils.createEdgeKey(
            SstUtils.getPartition(dstId, partitions),
            dstId,
            -edgeType,
            edgeRank,
            srcId,
            edgeVersion
          )

          val props = SstUtils.checkAndEncodeProps(propsDef, cols(2), cols(3))

          List(
            (key, props),
            (keyRev, props)
          )
     })
      .repartitionAndSortWithinPartitions(new SortByKeyPartitioner())
      .saveAsNewAPIHadoopFile[SSTFileOutputFormat](s"${outputPath}")
  }
}
