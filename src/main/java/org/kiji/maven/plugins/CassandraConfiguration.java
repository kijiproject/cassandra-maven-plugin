package org.kiji.maven.plugins;

import java.io.File;
import java.util.List;

import org.apache.maven.artifact.Artifact;

/**
 * Contains all of the user-configurable Cassandra configuration.
 */
public class CassandraConfiguration {
  private int numNodes;
  private int portNativeTransport;
  private int numVirtualNodes;
  private File cassandraDir;

  public int getPortStorage() {
    return portStorage;
  }

  public void setPortStorage(int portStorage) {
    this.portStorage = portStorage;
  }

  public int getPortSslStorage() {
    return portSslStorage;
  }

  public void setPortSslStorage(int portSslStorage) {
    this.portSslStorage = portSslStorage;
  }

  public int getPortRpc() {
    return portRpc;
  }

  public void setPortRpc(int portRpc) {
    this.portRpc = portRpc;
  }

  private int portStorage;
  private int portSslStorage;
  private int portRpc;

  public List<Artifact> getPluginDependencies() {
    return pluginDependencies;
  }

  public void setPluginDependencies(List<Artifact> pluginDependencies) {
    this.pluginDependencies = pluginDependencies;
  }

  private List<Artifact> pluginDependencies;

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

