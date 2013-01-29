package org.apache.hadoop.mapred.ompi;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.ompi.OmpiCommon;

/* the OmpiJobClient is the replacement for the Hadoop JobClient class
 * and executes the MR procedure using ORTE instead of YARN
 */
public class OmpiJobClient {
  public static final Log LOG = LogFactory.getLog(OmpiJobClient.class);
  int jobID;

  public int getJobId() {
    return jobID;
  }

  /* define the JNI interface for setting up a new jobclient */
  public native boolean initNative();

  /*
   * JNI interface - add comma-separated list of files to be colocated with
   * executables. Archives that end with .tar, .gz, .gzip, .bz2, or .bzip will
   * automatically be expanded on the remote end. Files will be placed in each
   * proc's working directory - name conflicts will generate an exception
   */
  public native void addFiles(String files);

  /*
   * JNI interface - add comma-separated list of jars to be colocated with
   * executables and added to CLASSPATH. Files will be placed in each proc's
   * working directory - name conflicts will generate an exception
   */
  public native void addJars(String jars);

  /* JNI interface - add environment */
  public native void addEnv(String env);

  /* JNI interface - add mapper */
  public native boolean addMapper(String[] args, String[] locations, Long length);

  /* JNI interface - add reducer */
  public native boolean addReducer(String[] args, String[] locations,
      Long length);

  /* JNI interface - run MR job, blocks until complete */
  public native boolean runJob();

  static public native String[] getActiveTrackerNames();

  static public native String[] getBlacklistedTrackerNames();

  static public native int getBlacklistedTrackers();

  static public native int getJobTrackerState();

  static public native int getMapTasks();

  static public native int getMaxMapTasks();

  static public native long getMaxmemory();

  static public native int getMaxReduceTasks();

  static public native int getReduceTasks();

  static public native int getTaskTrackers();

  static public native long getUsedMemory();

  /**
   * Blocks until the job is finished
   */
  public void waitForCompletion() throws IOException {
    // while (!isComplete()) {
    // try {
    // Thread.sleep(5000);
    // } catch (InterruptedException ie) {
    // }
    // }
  }

  /**
   * Create a job client.
   */
  public OmpiJobClient() {
    if (!initNative()) {
      /* throw an exception of some type */
      LOG.error("jobclient initNative failed");
    }
  }
}
