package org.kiji.maven.plugins;

import org.apache.maven.plugin.logging.Log;

/**
 * Interface for objects that can write to the maven log.
 */
public interface MavenLoggable {
  /**
   * Provides access to the maven log used to communicate with the user.
   *
   * @return The maven log.
   */
  Log getLog();
}
