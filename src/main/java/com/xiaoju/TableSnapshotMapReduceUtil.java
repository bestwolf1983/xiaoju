package com.xiaoju;

import com.yammer.metrics.core.MetricsRegistry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by cenyuhai on 2015/11/27.
 */
public class TableSnapshotMapReduceUtil extends TableMapReduceUtil {

  public static void initTableSnapshotMapperJob(String snapshotName, Scan scan,
                                                Class<? extends TableMapper> mapper,
                                                Class<?> outputKeyClass,
                                                Class<?> outputValueClass, Job job,
                                                boolean addDependencyJars, Path tmpRestoreDir) throws IOException {
    CustomTableSnapshotInputFormat.setInput(job, snapshotName, tmpRestoreDir);
    initTableMapperJob(snapshotName, scan, mapper, outputKeyClass,
        outputValueClass, job, addDependencyJars, false, CustomTableSnapshotInputFormat.class);
    addDependencyJars(job.getConfiguration(), MetricsRegistry.class);
    resetCacheConfig(job.getConfiguration());
  }
}
