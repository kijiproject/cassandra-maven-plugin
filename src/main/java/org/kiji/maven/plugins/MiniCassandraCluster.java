package org.kiji.maven.plugins;

import org.apache.maven.plugin.logging.Log;

/**
 * Represents the entire Cassandra cluster (possibly containing multiple nodes).
 */
public class MiniCassandraCluster extends MavenLogged {
  /** Whether the cluster is running. */
  private boolean mIsRunning;

  public MiniCassandraCluster(Log log) {
    super(log);
    mIsRunning = false;
  }

  public boolean isRunning() {
    return mIsRunning;
  }

  /**
   * Starts the cluster.  Blocks until ready.
   *
   * @throws Exception If there is an error.
   */
  public void startup() throws Exception {
    if (isRunning()) {
      throw new RuntimeException("Cluster already running.");
    }
    // TODO: Some stuff.
    mIsRunning = true;
  }

  /**
   * Stops the cluster.  Blocks until shut down.
   *
   * @throws Exception If there is an error.
   */
  public void shutdown() throws Exception {
    if (!mIsRunning) {
      getLog().error(
          "Attempting to shut down a cluster, but one was never started in this process.");
      return;
    }
    // TODO: Some stuff.
  }

}
