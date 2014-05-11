package org.kiji.maven.plugins;

/*
 * Copyright 2001-2005 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.maven.artifact.Artifact;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

/**
 * Maven goal to start a Cassandra cluster.
 */
@Mojo(
    name = "start",
    defaultPhase = LifecyclePhase.PRE_INTEGRATION_TEST
)
public class StartMojo extends AbstractMojo {
  /** If true, this goal should be a no-op. */
  @Parameter(property = "cassandra.skip", defaultValue = "false")
  private boolean mSkip;

  /** Number of nodes in the Cassandra cluster. */
  @Parameter(defaultValue = "1", alias = "numnodes")
  private int mNumNodes;

  /** Number of vnodes per Cassandra node. */
  @Parameter(property = "num.virtual.nodes", defaultValue = "256")
  private int mNumVirtualNodes;

  /** Directory into which to put all of the Cassandra stuff. */
  @Parameter(property = "cassandra.dir", defaultValue = "${project.build.directory}/cassandra-it")
  private File mCassandraDir;

  /** Dependencies for the plugin (needed for setting the classpath for Cassandra processes). */
  @Parameter(defaultValue="${plugin.artifacts}", readonly = true)
  private List<Artifact> pluginDependencies;

  /** IP address for node 0 (add 1 for every additional node's address). */
  @Parameter(alias = "initial.ip", defaultValue = "127.0.0.1")
  private String mInitialIpAddress;

  // -----------------------------------------------------------------------------------------------
  // Different port settings

  /** Port to use for Cassandra native transport. */
  @Parameter(alias = "native.port", defaultValue = "9042")
  private int mPortNativeTransport;

  /** Storage port. */
  @Parameter(alias = "storage.port", defaultValue = "7000")
  private int mPortStorage;

  /** SSL storage port. */
  @Parameter(alias = "ssl.storage.port", defaultValue = "7001")
  private int mPortSslStorage;

  /** RPC port. */
  @Parameter(alias = "rpc.port", defaultValue = "9160")
  private int mPortRpc;

  int getPortNativeTransport() {
    return mPortNativeTransport;
  }

  int getNumVnodes() {
    return mNumVirtualNodes;
  }

  /**
   * Starts a mini Cassandra cluster in a new set of threads.
   *
   * <p>This method is called by the maven plugin framework to run the goal.</p>
   *
   * @throws MojoExecutionException If there is a fatal error during this goal's execution.
   */
  @Override
  public void execute() throws MojoExecutionException {
    if (mSkip) {
      getLog().info("Not starting a Cassandra cluster because skip=true.");
      return;
    }

    //System.setProperty("java.class.path", getClassPath());
    //getLog().info("Set java.class.path to: " + System.getProperty("java.class.path"));

    // Start the cluster.
    try {
      MiniCassandraClusterSingleton.INSTANCE.startAndWaitUntilReady(
          getLog(),
          createCassandraConfiguration()
      );
    } catch (IOException e) {
      throw new MojoExecutionException("Unable to start Cassandra cluster.", e);
    }
  }

  private CassandraConfiguration createCassandraConfiguration() {
    CassandraConfiguration config = new CassandraConfiguration();
    config.setCassandraDir(mCassandraDir);
    config.setNumNodes(mNumNodes);
    config.setNumVirtualNodes(mNumVirtualNodes);
    config.setPortNativeTransport(mPortNativeTransport);
    config.setPluginDependencies(pluginDependencies);
    config.setPortRpc(mPortRpc);
    config.setPortSslStorage(mPortSslStorage);
    config.setPortStorage(mPortStorage);
    config.setInitialIpAddress(mInitialIpAddress);
    return config;
  }
}
