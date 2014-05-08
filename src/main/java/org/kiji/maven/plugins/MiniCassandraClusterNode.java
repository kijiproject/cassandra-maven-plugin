package org.kiji.maven.plugins;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.maven.plugin.logging.Log;
import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.IOUtil;
import org.yaml.snakeyaml.Yaml;

/**
 * Represents a single node in our Cassandra cluster.
 */
public class MiniCassandraClusterNode extends MavenLogged {

  /** Id number for this node. */
  private final int mNodeId;

  /** IP address for this node. */
  private final String mMyAddress;

  /** IP addresses for the other nodes. */
  private final List<String> mSeeds;

  /** Directory for this node. */
  private final File mRootDir;

  private final File mBinDir;
  private final File mConfDir;
  private final File mDataDir;
  private final File mCommitLogDir;
  private final File mSavedCachesDir;
  private final CassandraConfiguration mCassandraConfiguration;

  public MiniCassandraClusterNode(
      Log log,
      int nodeId,
      String myAddress,
      List<String> seeds,
      CassandraConfiguration configuration) {
    super(log);
    mNodeId = nodeId;
    mMyAddress = myAddress;
    mSeeds = seeds;
    mRootDir = new File(configuration.getCassandraDir(), "node-" + nodeId);
    mBinDir = new File(mRootDir, "bin");
    mConfDir = new File(mRootDir, "conf");
    mDataDir = new File(mRootDir, "data");
    mCommitLogDir = new File(mRootDir, "commitlog");
    mSavedCachesDir = new File(mRootDir, "saved_caches");
    mCassandraConfiguration = configuration;
  }

  /**
   * Set up all of the files and directories for this node.
   */
  public void setup() {
    createDirectories();
    try {
      createCassandraYaml();
    } catch (IOException ioe) {
      throw new RuntimeException("Problem creating YAML file.");
    }
  }

  private void createDirectories() {
    // Create directory for this node.
    Preconditions.checkArgument(new File(mRootDir.getParent()).isDirectory());
    try {
      FileUtils.deleteDirectory(mRootDir);
    } catch (IOException ioe) {
      throw new RuntimeException("Problem clearing out for directory " + mRootDir);
    }
    if (!mRootDir.mkdir()) {
      throw new RuntimeException("Problem creating directory " + mRootDir);
    }

    // Now create all of the subdirectories needed:
    for (File myDir : Arrays.asList(
        mBinDir, mConfDir, mDataDir, mCommitLogDir, mSavedCachesDir)) {
      if (!myDir.mkdir()) {
        throw new RuntimeException("Problem creating directory " + myDir);
      }
    }
  }

  private void createCassandraYaml() throws IOException {
    getLog().info("* Creating YAML for node " + mNodeId + ".");
    // Read the default Cassandra YAML file.
    String defaultYaml = IOUtil.toString(getClass().getResourceAsStream("/cassandra.yaml"));

    // Update some settings based on user configuration through maven.
    // Build a big string and then parse with YAML.
    String customYaml = createCustomYaml();

    // Overwrite any settings in the default YAML with our custom settings.
    String finalYaml = combineYaml(defaultYaml, customYaml);

    // Write out the new YAML.
    File cassandraYaml = new File(mConfDir, "cassandra.yaml");
    FileUtils.fileWrite(cassandraYaml.getAbsolutePath(), finalYaml);
  }

  /**
   * Update the settings in a baseline YAML description with settings from a new YAML description.
   *
   * Inspired by approach taken in Codehaus Cassandra Mojo.
   *
   * @param baselineYaml Baseline cassandra.yaml.
   * @param newYaml YAML settings to apply on top of baseline (possibly overriding settings in
   * baseline).
   * @return An updated YAML description.
   */
  private String combineYaml(String baselineYaml, String newYaml) {
    Yaml yaml = new Yaml();
    Map<String, Object> baselineMap = (Map<String, Object>) yaml.load(baselineYaml);
    Map<String, Object> newMap = (Map<String, Object>) yaml.load(newYaml);
    for (Map.Entry<String, Object> glossEntry : newMap.entrySet())
    {
      baselineMap.put(glossEntry.getKey(), glossEntry.getValue());
    }
    return yaml.dump(baselineMap);
  }

  /**
   * Build a YAML String containing a subset of a Cassandra YAML file based on the settings that
   * came from Maven.
   *
   */
  private String createCustomYaml() {
    StringBuilder sb = new StringBuilder();
    sb
        .append("data_file_directories:\n")
        .append("    - ")
        .append(mDataDir.getAbsolutePath())
        .append("\n");

    sb
        .append("commitlog_directory: ")
        .append(mCommitLogDir)
        .append("\n");

    sb
        .append("saved_caches_directory: ")
        .append(mSavedCachesDir).append("\n");

    sb
        .append("listen_address: ")
        .append(mMyAddress)
        .append("\n");

    // TODO: Allow custom RPC port, address.
    sb
        .append("rpc_address: ")
        .append(mMyAddress)
        .append("\n");
    sb
        .append("native_transport_port: ")
        .append(mCassandraConfiguration.getPortNativeTransport())
        .append("\n");

    sb
        .append("num_tokens: ")
        .append(mCassandraConfiguration.getNumVirtualNodes())
        .append("\n");

    if (mSeeds.size() != 0) {
      sb.append("seed_provider:\n");
      sb.append("    - class_name: org.apache.cassandra.locator.SimpleSeedProvider\n");
      sb.append("      parameters:\n");
      sb.append("          - seeds: \"");
      sb.append(Joiner.on(",").join(mSeeds));
      sb.append("\"\n");
    }
    return sb.toString();
  }

  /**
   * Start a dedicated embedded Cassandra service for this node.
   */
  public void start() {
    getLog().info("Starting node " + mNodeId);
    File cassandraYaml = new File(mConfDir, "cassandra.yaml");
    System.setProperty("cassandra.config", "file:" + cassandraYaml.getAbsolutePath());
    System.setProperty("cassandra-foreground", "true");

    EmbeddedCassandraService embeddedCassandraService = new EmbeddedCassandraService();
    try {
      embeddedCassandraService.start();
      getLog().info("Successfully started node " + mNodeId);
    } catch (IOException ioe) {
      getLog().warn("Could not start Cassandra node " + mNodeId);
    }
  }
}
