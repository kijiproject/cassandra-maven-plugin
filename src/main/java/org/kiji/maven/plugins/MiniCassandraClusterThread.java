package org.kiji.maven.plugins;

import org.apache.maven.plugin.logging.Log;

/**
 * A Thread to run a mini Cassandra cluster.
 */
public class MiniCassandraClusterThread extends Thread implements MavenLoggable {
  /** The maven log. */
  private final Log mLog;

  /** The local cassandra cluster. */
  private final MiniCassandraCluster mCassandraCluster;

  /** Whether the cluster is started and ready. */
  private volatile boolean mIsClusterReady;

  /** Whether the thread has been asked to stop. */
  private volatile boolean mIsStopRequested;

  /**
   * Creates a new <code>MiniCassandraClusterThread</code> instance.
   *
   * @param log The maven log.
   * @param cassandraCluster The cassandra cluster to run.
   */
  public MiniCassandraClusterThread(Log log, MiniCassandraCluster cassandraCluster) {
    mLog = log;
    mCassandraCluster = cassandraCluster;
    mIsClusterReady = false;
    mIsStopRequested = false;
  }

  /**
   * Determine whether the Cassandra cluster is up and running.
   *
   * @return Whether the cluster has completed startup.
   */
  public boolean isClusterReady() {
    return mIsClusterReady;
  }

  /**
   * Stops the Cassandra cluster gracefully.  When it is fully shut down, the thread will exit.
   */
  public void stopClusterGracefully() {
    mIsStopRequested = true;
    interrupt();
  }

  @Override
  public Log getLog() {
    return mLog;
  }

  /**
   * Runs the mini Cassandra cluster.
   *
   * <p>This method blocks until {@link #stopClusterGracefully()} is called.</p>
   */
  @Override
  public void run() {
    getLog().info("Starting up Cassandra cluster...");
    try {
      mCassandraCluster.startup();
    } catch (Exception e) {
      getLog().error("Unable to start a Cassandra cluster.", e);
      return;
    }
    getLog().info("Cassandra cluster started.");
    mIsClusterReady = true;
    yield();

    // Twiddle our thumbs until somebody requests the thread to stop.
    while (!mIsStopRequested) {
      try {
        sleep(1000);
      } catch (InterruptedException e) {
        getLog().debug("Main thread interrupted while waiting for cluster to stop.");
      }
    }

    getLog().info("Starting graceful shutdown of the Cassandra cluster...");
    try {
      mCassandraCluster.shutdown();
    } catch (Exception e) {
      getLog().error("Unable to stop the Cassandra cluster.", e);
      return;
    }
    getLog().info("Cassandra cluster shut down.");
  }
}

