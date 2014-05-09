package org.kiji.maven.plugins;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.maven.artifact.Artifact;
import org.apache.maven.artifact.ArtifactUtils;
import org.apache.maven.plugin.logging.Log;
import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.IOUtil;
import org.yaml.snakeyaml.Yaml;

/**
 * Represents a single node in our Cassandra cluster.
 */
public class MiniCassandraClusterNode extends MavenLogged {

  private static final String CLASSPATH_SEPARATOR = ":";

  /** Id number for this node. */
  private final int mNodeId;

  /** IP address for this node. */
  private final String mMyAddress;

  /** IP addresses for the other nodes. */
  private final List<String> mSeeds;

  /** Directory for this node. */
  private final File mRootDir;

  /** Process running a Cassandra Daemon. */
  private Process mCassandraProcess;

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
      createLog4jProperties();
    } catch (IOException ioe) {
      throw new RuntimeException("Problem creating YAML file.");
    }
  }

  private void createLog4jProperties() throws IOException {
    getLog().info("Creating LOG4J for node " + mNodeId + ".");

    StringBuilder sb = new StringBuilder();
    File systemLog = new File(mRootDir, "system.log");

    sb.append("log4j.rootLogger=INFO,stdout,R\n");
    sb
        .append("# stdout\n")
        .append("log4j.appender.stdout=org.apache.log4j.ConsoleAppender\n")
        .append("log4j.appender.stdout.layout=org.apache.log4j.PatternLayout\n")
        .append("log4j.appender.stdout.layout.ConversionPattern=%5p %d{HH:mm:ss,SSS} %m%n\n");

    sb
        .append("# rolling log file\n")
        .append("log4j.appender.R=org.apache.log4j.RollingFileAppender\n")
        .append("log4j.appender.R.maxFileSize=20MB\n")
        .append("log4j.appender.R.maxBackupIndex=50\n")
        .append("log4j.appender.R.layout=org.apache.log4j.PatternLayout\n")
        .append("log4j.appender.R.layout.ConversionPattern=%5p [%t] %d{ISO8601} %F (line %L) %m%n\n");

    sb
        .append("log4j.appender.R.File=")
        .append(systemLog.getAbsolutePath())
        .append("\n");

    sb.append("# Application logging options\n");
    sb
        .append("#log4j.logger.org.apache.cassandra=DEBUG\n")
        .append("#log4j.logger.org.apache.cassandra.db=DEBUG\n")
        .append("#log4j.logger.org.apache.cassandra.service.StorageProxy=DEBUG\n");

    sb.append("# Adding this to avoid thrift logging disconnect errors.\n");
    sb.append("log4j.logger.org.apache.thrift.server.TNonblockingServer=ERROR\n");

    String serverProperties = sb.toString();
    // Write out the new YAML.
    File cassandraLogs = new File(mConfDir, "log4j-server.properties");
    FileUtils.fileWrite(cassandraLogs.getAbsolutePath(), serverProperties);
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
    getLog().info("Creating YAML for node " + mNodeId + ".");
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

  private String getClasspath() {
    // Copied mostly from the Codehaus Cassandra plugin.

    StringBuilder cp = new StringBuilder();
    try {
      // we can't use StringUtils.join here since we need to add a '/' to
      // the end of directory entries - otherwise the jvm will ignore them.
      cp.append(new URL(mConfDir.toURI().toASCIIString()).toExternalForm());
      cp.append(CLASSPATH_SEPARATOR);
      /*
      getLog().debug( "Adding plugin artifact: " + ArtifactUtils.versionlessKey(pluginArtifact) +
          " to the classpath" );
      cp.append( new URL( pluginArtifact.getFile().toURI().toASCIIString() ).toExternalForm() );
      cp.append( ' ' );
      */

      for (Artifact artifact : mCassandraConfiguration.getPluginDependencies()) {
        getLog().debug(
            "Adding plugin dependency artifact: " + ArtifactUtils.versionlessKey( artifact ) +
            " to the classpath");
        // NOTE: if File points to a directory, this entry MUST end in '/'.
        cp.append(new URL(artifact.getFile().toURI().toASCIIString()).toExternalForm());
        cp.append(CLASSPATH_SEPARATOR);
      }
    } catch (MalformedURLException mue) {
      getLog().error("Could not create URL for conf dir " + mConfDir + "?!");
    }
    return cp.toString();
  }

  private String getJavaExecutable() {
    String separator = System.getProperty("file.separator");
    String classpath = System.getProperty("java.class.path");
    String path = System.getProperty("java.home")
        + separator + "bin" + separator + "java";
    return path;
  }

  private void updateEnvironmentVariables(Map<String, String> currentEnvironment) {

    currentEnvironment.put(
        "CASSANDRA_CONF",
        mConfDir.getAbsolutePath()
    );
  }

  /**
   * Start a dedicated Cassandra process for this node.
   */
  public void start() {
    getLog().info("Starting node " + mNodeId);
    try {
      ProcessBuilder pb = new ProcessBuilder();

      // Build a Java command line for running Cassandra.
      String javaExec = getJavaExecutable();

      // Set the classpath to include all of the dependencies for this plugin (i.e., Cassandra).
      String classpath = getClasspath();

      // Set CASSANDRA_CONF appropriately.
      Map<String, String> environmentVariables = pb.environment();
      updateEnvironmentVariables(environmentVariables);
      pb.command(
          javaExec,
          "-cp",
          classpath,
          CassandraDaemon.class.getCanonicalName()
      );
      pb.directory(mRootDir);
      mCassandraProcess = pb.start();
      getLog().info("Successfully started node " + mNodeId);
    } catch (IOException ioe) {
      getLog().warn("Could not start Cassandra node " + mNodeId);
    }
  }

  /**
   * Stop the process associated with this node.
   */
  public void stop() {
    mCassandraProcess.destroy();
    getLog().info("Stopped node " + mNodeId);
  }
}
