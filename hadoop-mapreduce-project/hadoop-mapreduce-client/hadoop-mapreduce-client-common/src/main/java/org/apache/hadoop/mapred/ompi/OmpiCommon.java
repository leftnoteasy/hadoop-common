package org.apache.hadoop.mapred.ompi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/* the OmpiJobClient is the replacement for the Hadoop JobClient class
 * and executes the MR procedure using ORTE instead of YARN
 */
public class OmpiCommon {
  public static final Log LOG = LogFactory.getLog(OmpiCommon.class);

  static {
    try {
      System.loadLibrary("mrplus");

      LOG.info("mrplus native library is available");
    } catch (UnsatisfiedLinkError ex) {
      throw ex;
    }
  }

  /*
   * define the JNI interface for initializing ORTE
   */
  public native boolean initNative(int flag);

  /*
   * define the JNI interface for finalization ORTE for jobclient
   */
  public native boolean finiNative();

  public native void passAlias(String aliasHosts);

  /**
   * Initialize OMPI
   * 
   * @param flag
   *          indicate it's HNP or APP
   * @param aliasHosts
   *          split by comma, a list of possible alias hostnames (only hostname,
   *          not include the DNS domains) of this host
   */
  public OmpiCommon(int flag, String aliasHosts) {
    if (!initNative(flag)) {
      /* throw an exception of some type */
      LOG.error("ompicommon initNative failed");
    }
    passAlias(aliasHosts);
  }

  public OmpiCommon(int flag) {
    this(flag, null);
  }

  public static void main(String[] args) {
    OmpiCommon common = new OmpiCommon(0);
  }
}