package org.kiji.fiddle;


import java.lang.RuntimeException;

import static org.junit.Assert.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple App.
 */
public class AppIT {
  private static final Logger LOG = LoggerFactory.getLogger(AppIT.class);
  @Test
  public void simpleTest() {
    try {
      Thread.sleep(1000 * 1);
    } catch (InterruptedException ie) {
    }
    Cluster cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build();
    Session session = cluster.connect();
    LOG.info("Opened connection to cluster!");
    System.out.println("Running test");
    session.close();
    cluster.close();
  }
}
