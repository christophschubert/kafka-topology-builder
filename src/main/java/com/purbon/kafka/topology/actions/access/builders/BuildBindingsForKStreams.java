package com.purbon.kafka.topology.actions.access.builders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.actions.BaseAccessControlAction;
import com.purbon.kafka.topology.model.users.KStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BuildBindingsForKStreams extends BaseAccessControlAction {

  private final BindingsBuilderProvider builderProvider;
  private final KStream app;
  private final String topicPrefix;

  public BuildBindingsForKStreams(
      BindingsBuilderProvider builderProvider, KStream app, String topicPrefix) {
    super();
    this.builderProvider = builderProvider;
    this.app = app;
    this.topicPrefix = topicPrefix;
  }

  @Override
  public void run() throws IOException {
    List<String> readTopics = app.getTopics().get(KStream.READ_TOPICS);
    List<String> writeTopics = app.getTopics().get(KStream.WRITE_TOPICS);

    bindings =
        builderProvider.buildBindingsForStreamsApp(
            app.getPrincipal(), topicPrefix, readTopics, writeTopics);
  }

  @Override
  protected Map<String, Object> props() {
    Map<String, Object> map = new HashMap<>();
    map.put("Operation", getClass().getName());
    map.put("Principal", app.getPrincipal());
    map.put("Topic", topicPrefix);
    return map;
  }
}
