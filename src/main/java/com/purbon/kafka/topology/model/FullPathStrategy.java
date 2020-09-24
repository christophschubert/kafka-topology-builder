package com.purbon.kafka.topology.model;

import java.util.StringJoiner;

public class FullPathStrategy implements TopicNamingStrategy {

  private final String topicNameComponentSeparator;

  public FullPathStrategy(String topicNameComponentSeparator) {
    this.topicNameComponentSeparator = topicNameComponentSeparator;
  }

  @Override
  public String buildTopicName(Topic topic) {
    final StringJoiner nameComponents =
        new StringJoiner(topicNameComponentSeparator)
            .add(buildTopicPrefix(topic.getProject()))
            .add(topic.getName());
    topic.getDataType().ifPresent(nameComponents::add);
    return nameComponents.toString();
  }

  @Override
  public String buildTopicPrefix(Project project) {
    final Topology topology = project.getTopology();
    final StringJoiner nameComponents =
        new StringJoiner(topicNameComponentSeparator).add(topology.getContext());
    topology.otherNameComponents().forEach((_k, v) -> nameComponents.add(v));
    nameComponents.add(project.getName());
    return nameComponents.toString();
  }
}
