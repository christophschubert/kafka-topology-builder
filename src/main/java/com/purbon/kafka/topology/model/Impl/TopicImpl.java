package com.purbon.kafka.topology.model.Impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.purbon.kafka.topology.TopicManager;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.TopicSchemas;
import java.util.*;

public class TopicImpl implements Topic, Cloneable {

  public static final String DEFAULT_TOPIC_NAME = "default";

  @JsonInclude(Include.NON_EMPTY)
  private Optional<String> dataType;

  @JsonInclude(Include.NON_EMPTY)
  private TopicSchemas schemas;

  private String name;

  private HashMap<String, String> config;

  private int partitionCount;
  private int replicationFactor;

  private Project project;

  private String projectPrefix;

  public TopicImpl(String name) {
    this(name, Optional.empty(), new HashMap<>());
  }

  public TopicImpl(String name, String dataType) {
    this(name, Optional.of(dataType), new HashMap<>());
  }

  public TopicImpl(String name, Optional<String> dataType, HashMap<String, String> config) {
    this.name = name;
    this.dataType = dataType;
    this.config = config;
    this.replicationFactor = 0;
    this.partitionCount = 0;
    this.project = null;
  }

  public TopicImpl() {
    this(DEFAULT_TOPIC_NAME, Optional.empty(), new HashMap<>());
  }

  public String getName() {
    return name;
  }

  public TopicSchemas getSchemas() {
    return schemas;
  }

  public void setSchemas(TopicSchemas schemas) {
    this.schemas = schemas;
  }

  @Override
  public String toString() {
    return project.getTopology().buildQualifiedTopicName(this);
  }

  public void setName(String name) {
    this.name = name;
  }

  public HashMap<String, String> getConfig() {
    return config;
  }

  public void setConfig(HashMap<String, String> config) {
    this.config = config;
  }

  public Map<String, String> rawConfig() {
    String value = getConfig().remove(TopicManager.NUM_PARTITIONS);
    if (value != null) {
      partitionCount = Integer.valueOf(value);
    }
    value = getConfig().remove(TopicManager.REPLICATION_FACTOR);
    if (value != null) {
      replicationFactor = Integer.valueOf(value);
    }
    return getConfig();
  }

  public Optional<String> getDataType() {
    return dataType;
  }

  @JsonIgnore
  @Override
  public Project getProject() {
    return project;
  }

  @Override
  public void setProject(Project project) {
    this.project = project;
  }

  @Override
  public int partitionsCount() {
    String configValue = getConfig().get(TopicManager.NUM_PARTITIONS);
    if (configValue == null) {
      return partitionCount;
    } else {
      return Integer.valueOf(configValue);
    }
  }

  @Override
  public Topic clone() {
    try {
      return (Topic) super.clone();
    } catch (CloneNotSupportedException e) {
      return new TopicImpl(getName(), getDataType(), getConfig());
    }
  }
}
