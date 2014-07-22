package org.kiji.fiddle;


import java.lang.RuntimeException;

import static org.junit.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Unit test for simple App.
 */
public class AppIT {
  private static final Logger LOG = LoggerFactory.getLogger(AppIT.class);
  @Test
  public void simpleTest() {
    // Load properties file from classpath.
    Properties prop = new Properties();
    InputStream input = null;

    try {
      input =
          this.getClass().getClassLoader().getResourceAsStream("cassandra-maven-plugin.properties");

      // load a properties file
      prop.load(input);

      input.close();

      Cluster cluster = Cluster.builder()
          .addContactPoint(prop.getProperty("cassandra.initialIp"))
          .withPort(Integer.parseInt(prop.getProperty("cassandra.nativePort")))
          .build();
      Session session = cluster.connect();
      LOG.info("Opened connection to cluster!");
      System.out.println("Running test");
      session.close();
      cluster.close();
    } catch (Exception e) {
      LOG.error(e.getMessage());
      org.junit.Assert.assertFalse("Failed if we got here", true);
    }
  }
}
