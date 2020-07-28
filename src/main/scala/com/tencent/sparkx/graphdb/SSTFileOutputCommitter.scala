package com.tencent.sparkx.graphdb

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce._
import org.slf4j.LoggerFactory

class SSTFileOutputCommitter(fs: FileSystem, outputPath: Path) extends OutputCommitter {
  private val log = LoggerFactory.getLogger(getClass)

  val PENDING_DIR_NAME = "_temporary"

  private def getPendingJobAttemptsPath: Path = {
    new Path(outputPath, PENDING_DIR_NAME)
  }

  private def getAppAttemptId(context: JobContext): Int = {
    context.getConfiguration.getInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0)
  }

  def getJobAttemptPath(context: JobContext): Path = {
    new Path(getPendingJobAttemptsPath, String.valueOf(getAppAttemptId(context)))
  }

  private def getPendingTaskAttemptsPath(context: JobContext): Path = {
    new Path(getJobAttemptPath(context), PENDING_DIR_NAME)
  }

  def getTaskAttemptPath(context: TaskAttemptContext): Path = {
    new Path(getPendingTaskAttemptsPath(context), String.valueOf(context.getTaskAttemptID))
  }

  @throws[IOException]
  private def mergePaths(from: Path, to: Path): Unit = {
    log.debug(s"Merging data from ${from} to ${to}")
    if (fs.isFile(from)) {
      if (fs.exists(to) && !fs.delete(to, true)) {
        throw new IOException("Failed to delete " + to)
      }
      if (!fs.rename(from, to)) {
        throw new IOException("Failed to rename " + from + " to " + to)
      }
    } else if (fs.isDirectory(from)) {
      if (fs.exists(to)) {
        if (!fs.isDirectory(to)) {
          if (!fs.delete(to, true)) {
            throw new IOException("Failed to delete " + to)
          }
          fs.mkdirs(to)
        }
      } else {
        fs.mkdirs(to)
      }
      // It is a directory so merge everything in the directories
      for (subFrom <- fs.listStatus(from)) {
        val subTo = new Path(to, subFrom.getPath.getName)
        mergePaths(subFrom.getPath, subTo)
      }
    }
  }

  @throws[IOException]
  override def setupJob(context: JobContext): Unit = {
    val jobAttemptPath = getJobAttemptPath(context)
    if (fs.exists(outputPath) && !fs.delete(outputPath, true)) {
      throw new IOException("Could not delete " + outputPath)
    }
    if (!fs.mkdirs(jobAttemptPath)) {
      log.error("Mkdirs failed to create " + jobAttemptPath)
    }
  }

  @throws[IOException]
  override def commitJob(context: JobContext): Unit = {
    fs.delete(getPendingJobAttemptsPath, true)
  }

  @throws[IOException]
  override def abortJob(context: JobContext, state: JobStatus.State): Unit = {
    fs.delete(getPendingJobAttemptsPath, true)
  }

  @throws[IOException]
  override def setupTask(context: TaskAttemptContext): Unit = {
    // SSTFileOutputCommitter's setupTask doesn't do anything. Because the
    // temporary task directory is created on demand when the
    // task is writing.
  }

  @throws[IOException]
  override def commitTask(context: TaskAttemptContext): Unit = {
    context.progress()
    mergePaths(getTaskAttemptPath(context), outputPath)
  }

  @throws[IOException]
  override def abortTask(context: TaskAttemptContext): Unit = {
    context.progress()
    fs.delete(getTaskAttemptPath(context), true)
  }

  @throws[IOException]
  override def needsTaskCommit(context: TaskAttemptContext): Boolean = {
    fs.exists(getTaskAttemptPath(context))
  }
}
