package com.xiaoju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;

import java.io.IOException;

/**
 * Created by cenyuhai on 2015/11/27.
 */
public class CustomRestoreSnapshotHelper {
  public static void copySnapshotForScanner(Configuration conf, FileSystem fs, Path rootDir,
                                            Path restoreDir, String snapshotName) throws IOException {

    if (restoreDir.toUri().getPath().startsWith(rootDir.toUri().getPath())) {
      throw new IllegalArgumentException("Restore directory cannot be a sub directory of HBase " +
          "root directory. RootDir: " + rootDir + ", restoreDir: " + restoreDir);
    }

    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    HBaseProtos.SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);

    MonitoredTask status = TaskMonitor.get().createStatus(
        "Restoring  snapshot '" + snapshotName + "' to directory " + restoreDir);
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher();

    RestoreSnapshotHelper helper = new RestoreSnapshotHelper(conf, fs,
        manifest, manifest.getTableDescriptor(), restoreDir, monitor, status);
    helper.restoreHdfsRegions(); // TODO: parallelize.


  }
}
