package org.kiji.maven.plugins;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.maven.plugin.logging.Log;

/**
 * Represents the entire Cassandra cluster (possibly containing multiple nodes).
 */
public class MiniCassandraCluster extends MavenLogged {
  /** Whether the cluster is running. */
  private boolean mIsRunning;

  private CassandraConfiguration mCassandraConfiguration;

  private Collection<MiniCassandraClusterNode> mNodes;

  public MiniCassandraCluster(Log log, CassandraConfiguration config) {
    super(log);
    mCassandraConfiguration = config;
    mIsRunning = false;
  }

  public boolean isRunning() {
    return mIsRunning;
  }

  private List<String> getSeeds() {
    List<String> seeds = Lists.newArrayList();

    // Get the base of the IP address.
    List<String> ipComponents = Lists.newArrayList(
        Splitter.on(".").split(mCassandraConfiguration.getInitialIpAddress())
    );

    if (ipComponents.size() != 4) {
      throw new IllegalArgumentException("Looks like " +
          mCassandraConfiguration.getInitialIpAddress() +
          " is not a legal IP address.");
    }
    int ipStart;
    try {
      ipStart = Integer.parseInt(ipComponents.get(3));
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Looks like " +
          mCassandraConfiguration.getInitialIpAddress() +
          " is not a legal IP address.");
    }

    for (int nodeNum = 0; nodeNum < mCassandraConfiguration.getNumNodes(); nodeNum++) {
      seeds.add(String.format("%s.%s.%s.%s",
          ipComponents.get(0),
          ipComponents.get(1),
          ipComponents.get(2),
          nodeNum + ipStart
          ));
    }
    return seeds;
  }

  /**
   * @return Whether it is currently possible to connect to the Cassandra cluster.
   */
  private boolean ableToConnectToCluster() {
    List<String> seeds = getSeeds();
    boolean connected;

    connected = false;
    try {
      // Sanity check that we *cannot* connect to the cluster before starting.
      Cluster cluster = Cluster.builder()
          .addContactPoints(seeds.toArray(new String[seeds.size()]))
          .withPort(mCassandraConfiguration.getPortNativeTransport())
          .build();
      getLog().debug(String.format(
          "Trying to connect using addresses %s and port %s",
          seeds,
          mCassandraConfiguration.getPortNativeTransport()
      ));
      Session session = cluster.connect();
      connected = true;
      session.close();
      cluster.close();
    } catch (RejectedExecutionException ree) {
      // This should never happen - Give the user a friendly message.
      getLog().error("RejectedExecutionException... (If you are on OS X, try running "
              + "sudo ifconfig lo0 alias 127.0.0.[0-15]");
    } catch (Exception e) {
      // Don't do anything.  Whatever code called this code will handle the lack of a connection.
    }
    return connected;
  }

  /**
   * @return whether all of the underlying Cassandra processes for the nodes in this cluster are
   *     are still alive.
   */
  private boolean allNodeProcessesAreAlive() {
    for (MiniCassandraClusterNode node: mNodes) {
      if (!node.isRunning()) {
        getLog().error("Process for Cassandra node " + node + " died during plugin startup.");
        return false;
      }
    }
    return true;
  }

  /**
   * Set up the Cassandra integration test directory, along with whatever per-node setup is
   * necessary.
   */
  private void initializeCassandraDirectories() {
    final File cassandraRootDir = mCassandraConfiguration.getCassandraDir();

    // Delete the existing directory, if it exists.
    if (cassandraRootDir.exists()) {
      try {
        org.apache.commons.io.FileUtils.forceDelete(cassandraRootDir);
      } catch (IOException ioe) {
        throw new RuntimeException("Could not create root Cassandra dir " + cassandraRootDir);
      }
    }

    // Create root directory.
    if (!cassandraRootDir.mkdir()) {
      throw new RuntimeException("Could not create root Cassandra dir " + cassandraRootDir);
    }

    // Set up all of the different conf directories.
    for (MiniCassandraClusterNode node : mNodes) {
      node.setup();
    }
  }

  /**
   * Create the per-node `MiniCassandraClusterNode` objects for this `MiniCassandraCluster`.
   */
  private void createNodeObjects() {
    // TODO: Check that the number of nodes is legal...
    List<String> seeds = getSeeds();

    mNodes = Lists.newArrayList();

    // Create a separate object for each node in the cluster.
    for (int nodeNum = 0; nodeNum < mCassandraConfiguration.getNumNodes(); nodeNum++) {
      mNodes.add(new MiniCassandraClusterNode(
              getLog(),
              nodeNum,
              seeds.get(nodeNum),
              seeds,
              mCassandraConfiguration));
    }
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

    // Create the actual node objects (each has a different node ID, IP address, etc.)
    createNodeObjects();

    // Create Yaml, properties files, etc. for each node.
    initializeCassandraDirectories();

    // We should not yet be able to connect to the cluster!
    if (ableToConnectToCluster()) {
      throw new RuntimeException("Failure during sanity check before starting Cassandra cluster.");
    }

    // Actually start the nodes!
    for (MiniCassandraClusterNode node : mNodes) {
      node.start();
    }

    mIsRunning = true;

    // Wait for the cluster to start running.

    // Allow a maximum of 5 minutes to start the entire cluster.
    // Poll every 10 seconds.
    final int maxNumTries = 30;
    final int sleepTimeSeconds = 10;
    boolean connected = false;
    boolean alive = false;
    for (int numTries = 0; numTries < maxNumTries; numTries++) {
      Thread.sleep(1000 * sleepTimeSeconds);
      // Check whether we are now able to connect to the cluster.
      connected = ableToConnectToCluster();
      // Check that the process for all of the cluster nodes are still alive.
      alive = allNodeProcessesAreAlive();
      if (connected || !alive) {
        break;
      }
    }

    if (!alive) {
      throw new RuntimeException("At least one of the Cassandra processes died during startup.");
    }
    if (!connected) {
      throw new RuntimeException("Cassandra cluster should be up now, but cannot connect!");
    } else {
      getLog().info("Test connection to Cassandra successful -- cluster is up!");
    }
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
    for (MiniCassandraClusterNode node : mNodes) {
      node.stop();
    }
  }

}
