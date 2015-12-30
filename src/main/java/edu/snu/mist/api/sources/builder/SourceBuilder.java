package edu.snu.mist.api.sources.builder;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface defines commonly necessary methods for building MIST SourceStream.
 */
@DefaultImplementation(SourceBuilderImpl.class)
public interface SourceBuilder {
  /**
   * Get the target source type of this builder.
   * @return The type of source it configures. Ex) ReefNetworkSource
   */
  String getSourceType();

  /**
   * Build key-value configuration for MIST SourceStream.
   * @return Key-value configuration
   */
  SourceConfiguration build();

  /**
   * Sets the configuration for the given param to the given value.
   * @param param
   * @param value
   * @return
   */
  SourceBuilder set(String param, Object value);
}