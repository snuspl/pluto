package edu.snu.mist.api.sources.builder;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface defines necessary methods for storing and getting
 * configuration for source streams.
 */
@DefaultImplementation(DefaultSourceConfigurationImpl.class)
public interface SourceConfiguration {

  /**
   * Gets the configuration value for the given parameter.
   * @param parameter
   * @return
   */
  Object getConfigurationValue(String parameter);
}