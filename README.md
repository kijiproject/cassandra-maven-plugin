Cassandra Maven Plugin
======================

A maven plugin that starts/stops a mini Cassandra cluster.

This plugin is useful for integration testing code that interacts with an HBase cluster.  Typically,
you will bind the `start` goal to your `pre-integration-test` phase and the `stop` goal to the
`post-integration-test` phase of the maven build lifecycle.

For an example of the usage of this plugin, see the integration test in this project.

