package com.xiaoju;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by cenyuhai on 2015/11/27.
 */
public class CustomTableSnapshotInputFormat extends TableSnapshotInputFormat {
  public static void setInput(Job job, String snapshotName, Path restoreDir) throws IOException {
    CustomTableSnapshotInputFormatImpl.setInput(job.getConfiguration(), snapshotName, restoreDir);
  }
}
