package org.kiji.maven.plugins;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.maven.plugin.logging.Log;

/**
 * Represents the entire Cassandra cluster (possibly containing multiple nodes).
 */
public class MiniCassandraCluster extends MavenLogged {
  /** Whether the cluster is running. */
  private boolean mIsRunning;

  private CassandraConfiguration mCassandraConfiguration;

  public MiniCassandraCluster(Log log, CassandraConfiguration config) {
    super(log);
    mCassandraConfiguration = config;
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

    // TODO: Check that the number of nodes is legal...
    List<String> seeds = Lists.newArrayList();
    for (int nodeNum = 1; nodeNum < 1+mCassandraConfiguration.getNumNodes(); nodeNum++) {
      seeds.add("127.0.0." + nodeNum);
    }

    List<MiniCassandraClusterNode> nodes = Lists.newArrayList();

    // Create a separate object for each node in the cluster.
    for (int nodeNum = 0; nodeNum < mCassandraConfiguration.getNumNodes(); nodeNum++) {
      nodes.add(new MiniCassandraClusterNode(
          getLog(),
          nodeNum,
          seeds.get(nodeNum),
          seeds,
          mCassandraConfiguration));
    }

    // Create root directory.
    if (!mCassandraConfiguration.getCassandraDir().mkdir()) {
      throw new RuntimeException("Could not create root C* dir " + mCassandraConfiguration.getCassandraDir());
    }

    // Set up all of the different conf directories.
    for (MiniCassandraClusterNode node : nodes) {
      node.setup();
    }

    // Actually start the nodes!
    for (MiniCassandraClusterNode node : nodes) {
      node.start();
    }

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
