package com.tencent.sparkx.graphdb

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InvalidJobConfException
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.security.TokenCache
import org.apache.spark.Partitioner
import org.rocksdb._
import org.slf4j.LoggerFactory

class SSTFileOutputFormat extends OutputFormat[Array[Byte], Array[Byte]] {
  private val log = LoggerFactory.getLogger(getClass)

  override def getRecordWriter(context: TaskAttemptContext):
  RecordWriter[Array[Byte], Array[Byte]] = {
    val sparkPartitionId = context.getTaskAttemptID.getTaskID.getId

    val outputDir = getOutputCommitter(context)
      .asInstanceOf[SSTFileOutputCommitter]
      .getTaskAttemptPath(context)
    val outputFs = outputDir.getFileSystem(context.getConfiguration)

    val localDir = new Path(s"${System.getProperty("user.dir")}/.cache")
    val localFs = localDir.getFileSystem(new Configuration(false))
    if (!localFs.exists(localDir) && !localFs.mkdirs(localDir)) {
      log.error(s"mkdir failed. ${localDir}")
      throw new IOException(s"mkdir failed. ${localDir}")
    }

    new RecordWriter[Array[Byte], Array[Byte]] {
      // all RocksObject should be closed
      private val env = new EnvOptions
      private val options = new Options()
        .setCreateIfMissing(true)
        // rocksdb jni has no compression.
        // .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
        .useCappedPrefixExtractor(16)
        .setTableFormatConfig(
          new BlockBasedTableConfig()
            .setBlockSize(512 << 10)
            .setWholeKeyFiltering(false)
            // .setEnableIndexCompression(true)
            .setBlockRestartInterval(64)
            .setUseDeltaEncoding(true)
            .setFilterPolicy(new BloomFilter(10, false))
            .setIndexType(IndexType.kTwoLevelIndexSearch)
            .setIndexBlockRestartInterval(16)
            .setPartitionFilters(true)
            .setFormatVersion(4)
            .setNoBlockCache(true)
            .setPinL0FilterAndIndexBlocksInCache(false)
            .setCacheIndexAndFilterBlocks(false)
            .setPinTopLevelIndexAndFilter(false)
        )

      var sstFileWriter: SstFileWriter = _
      var graphPartitionId: Int = _
      var localFile: Path = _
      var outputFile: Path = _
      private var cnt: Long = 0
      private var lastKey: Array[Byte] = Array[Byte](0)

      @throws[IOException]
      @throws[InterruptedException]
      override def write(key: Array[Byte], value: Array[Byte]): Unit = {
        if (cnt == 0) {
          graphPartitionId = SstUtils.getPartition(key)
          val sstFileName = s"${graphPartitionId}-${sparkPartitionId}.sst"
          localFile = new Path(localDir, s"${sstFileName}.${context.getTaskAttemptID}")
          outputFile = new Path(outputDir, s"${graphPartitionId}/${sstFileName}")
          log.info(s"sst localFile ${localFile}, outputFile= ${outputFile}")

          sstFileWriter = new SstFileWriter(env, options)
          try {
            sstFileWriter.open(localFile.toString)
          } catch {
            case e: Exception =>
              log.error(s"sstFileWriter.open(${localFile}) Failed. ${e.getMessage}")
            case e: Error =>
              log.error(s"sstFileWriter.open(${localFile}) Failed. ${e.getMessage}")
          }
        }
        assert(graphPartitionId == SstUtils.getPartition(key))
        if (!key.sameElements(lastKey)) {
          if (value == null) {
            sstFileWriter.put(key, SstUtils.emptyValueEncoded)
          } else {
            sstFileWriter.put(key, value)
          }
        }
        lastKey = key
        cnt += 1
      }

      @throws[IOException]
      @throws[InterruptedException]
      override def close(context: TaskAttemptContext): Unit = {
        log.info(s"try finish cnt[${cnt}] ${localFile}")
        if (cnt != 0) {
          sstFileWriter.finish()
          sstFileWriter.close()

          log.info(
            s"size ${SstUtils.humanReadableByteSize(localFs.getFileStatus(localFile).getLen)}. " +
              s"start copy. local ${localFile} out ${outputFile}")
          outputFs.copyFromLocalFile(true, true, localFile, outputFile)
          log.info(s"finish copy. local ${localFile} out ${outputFile}")
        }
        env.close()
        options.close()
      }
    }
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def checkOutputSpecs(job: JobContext): Unit = {
    val outDir: Path = FileOutputFormat.getOutputPath(job)
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set.")
    }

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(
      job.getCredentials, Array[Path](outDir), job.getConfiguration)
  }

  private var committer: SSTFileOutputCommitter = _

  @throws[IOException]
  @throws[InterruptedException]
  override def getOutputCommitter(context: TaskAttemptContext): OutputCommitter = {
    if (committer == null) {
      val output = FileOutputFormat.getOutputPath(context)
      val fs = output.getFileSystem(context.getConfiguration)
      committer = new SSTFileOutputCommitter(fs, fs.makeQualified(output))
    }
    committer
  }
}

case class SstPartConfig(partitions: Int, subPartitions: Int)

class SortByKeyPartitioner()(private implicit val c: SstPartConfig)
  extends Partitioner {
  override def numPartitions: Int = c.partitions * c.subPartitions

  override def getPartition(key: Any): Int = {
    val keyEncoded = key.asInstanceOf[Array[Byte]]
    val graphPartitionId = SstUtils.getPartition(keyEncoded)
    val vid = SstUtils.getVId(keyEncoded)
    assert(graphPartitionId == SstUtils.getPartition(vid, c.partitions))
    val graphPartitionIdSubId = (SstUtils.getPartition(vid, numPartitions) - 1) / c.partitions
    val sparkPartitionId = c.subPartitions * (graphPartitionId - 1) + graphPartitionIdSubId
    assert(graphPartitionId == sparkPartitionId / c.subPartitions + 1)
    assert(graphPartitionIdSubId == sparkPartitionId % c.subPartitions)
    sparkPartitionId
  }
}

