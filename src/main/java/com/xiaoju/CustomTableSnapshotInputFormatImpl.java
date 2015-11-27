package com.xiaoju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormatImpl;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;

import java.io.IOException;
import java.util.UUID;

/**
 * Created by cenyuhai on 2015/11/27.
 */
public class CustomTableSnapshotInputFormatImpl extends TableSnapshotInputFormatImpl{
  /**
   * Configures the job to use TableSnapshotInputFormat to read from a snapshot.
   * @param conf the job to configure
   * @param snapshotName the name of the snapshot to read from
   * @param restoreDir a temporary directory to restore the snapshot into. Current user should
   * have write permissions to this directory, and this should not be a subdirectory of rootdir.
   * After the job is finished, restoreDir can be deleted.
   * @throws IOException if an error occurs
   */
  public static void setInput(Configuration conf, String snapshotName, Path restoreDir)
      throws IOException {
    conf.set("hbase.TableSnapshotInputFormat.snapshot.name", snapshotName);

    Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
    FileSystem fs = rootDir.getFileSystem(conf);

    restoreDir = new Path(restoreDir, UUID.randomUUID().toString());

    // TODO: restore from record readers to parallelize.
    CustomRestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);

    conf.set("hbase.TableSnapshotInputFormat.restore.dir", restoreDir.toString());
  }
}
