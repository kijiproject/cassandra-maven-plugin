package org.kiji.maven.plugins;

import java.io.IOException;

import org.apache.maven.plugin.logging.Log;

/**
 * A singleton instance of a mini Cassandra cluster.
 */
public enum MiniCassandraClusterSingleton {
  /** The singleton instance. */
  INSTANCE;

  /** The thread that runs the mini C* cluster. */
  private MiniCassandraClusterThread mThread;

  /** The C* cluster being run. */
  private MiniCassandraCluster mCluster;

  /**
   * Starts the C* cluster and blocks until it is ready.
   *
   * @param log The maven log.
   * @throws java.io.IOException If there is an error.
   */
  public void startAndWaitUntilReady(Log log) throws IOException {
    // TODO: Add support for also starting a mapreduce cluster.
    mCluster = new MiniCassandraCluster(log);
    mThread = new MiniCassandraClusterThread(log, mCluster);

    log.info("Starting new thread...");
    mThread.start();

    // Wait for the cluster to be ready.
    log.info("Waiting for cluster to be ready...");
    while (!mThread.isClusterReady()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.info("Still waiting...");
      }
    }
    log.info("Finished waiting for Cassandra cluster thread.");
  }

  /**
   * Stops the Cassandra cluster and blocks until is has been shutdown completely.
   *
   * @param log The maven log.
   */
  public void stop(Log log) {
    if (null == mCluster) {
      log.error("Attempted to stop a cluster, but no cluster was ever started in this process.");
      return;
    }

    log.info("Stopping the Cassandra cluster thread...");
    mThread.stopClusterGracefully();
    while (mThread.isAlive()) {
      try {
        mThread.join();
      } catch (InterruptedException e) {
        log.debug("Cassandra cluster thread interrupted.");
      }
    }
    log.info("Cassandra cluster thread stopped.");
  }
}

