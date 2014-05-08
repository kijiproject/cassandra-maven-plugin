package org.kiji.maven.plugins;

import java.io.File;

/**
 * Contains all of the user-configurable Cassandra configuration.
 */
public class CassandraConfiguration {
  private int numNodes;

  private int portNativeTransport;

  private int numVirtualNodes;

  private File cassandraDir;

  public void setNumNodes(int numNodes) {
    this.numNodes = numNodes;
  }
  public int getNumNodes() {
    return numNodes;
  }

  public void setPortNativeTransport(int portNativeTransport) {
    this.portNativeTransport = portNativeTransport;
  }
  public int getPortNativeTransport() {
    return portNativeTransport;
  }

  public void setCassandraDir(File cassandraDir) {
    this.cassandraDir = cassandraDir;
  }

  public File getCassandraDir() {
    return cassandraDir;
  }

  public void setNumVirtualNodes(int numVirtualNodes) {
    this.numVirtualNodes = numVirtualNodes;
  }
  public int getNumVirtualNodes() {
    return numVirtualNodes;
  }
}

