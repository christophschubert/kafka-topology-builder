package com.purbon.kafka.topology.model;

public interface TopicNamingStrategy {
  String buildTopicName(Topic topic);

  String buildTopicPrefix(Project project);
}
