package edu.snu.mist.api.sources.builder;

import java.util.HashMap;
import java.util.Map;

/**
 * The default implementation class for SourceConfiguration.
 */
public class DefaultSourceConfigurationImpl implements SourceConfiguration {

  private final Map<String, Object> configMap;

  public DefaultSourceConfigurationImpl(final Map<String, Object> configMap) {
    this.configMap = new HashMap<>();
    configMap.keySet().stream()
        .forEach(s->this.configMap.put(s, configMap.get(s)));
  }

  @Override
  public Object getConfigurationValue(final String param) {
    if (!configMap.containsKey(param)) {
      throw new IllegalStateException("Tried to get a configuration value for non-existing param!");
    }
    return configMap.get(param);
  }
}
