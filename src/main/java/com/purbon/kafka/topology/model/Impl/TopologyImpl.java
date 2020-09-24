package com.purbon.kafka.topology.model.Impl;

import com.purbon.kafka.topology.model.*;
import java.util.*;

public class TopologyImpl implements Topology, Cloneable {

  private final String topicNameComponentSeparator = ".";
  private final TopicNamingStrategy topicNamingStrategy = new FullPathStrategy(".");

  private String context;

  // we rely on insertion order to elements
  private LinkedHashMap<String, String> others;

  private List<Project> projects;
  private Platform platform;

  public TopologyImpl() {
    this.context = "default";
    this.others = new LinkedHashMap<>();
    this.projects = new ArrayList<>();
    this.platform = new Platform();
  }

  public String getContext() {
    return context;
  }

  public void setContext(String context) {
    this.context = context;
  }

  public List<Project> getProjects() {
    return projects;
  }

  public void addProject(Project project) {
    project.setTopology(this);
    this.projects.add(project);
  }

  public void setProjects(List<Project> projects) {
    this.projects = new ArrayList<>(projects);
    this.projects.forEach(project -> project.setTopology(this));
  }

  public void addOther(String fieldName, String value) {
    others.put(fieldName, value);
  }

  @Override
  public LinkedHashMap<String, String> otherNameComponents() {
    return others;
  }

  public void setPlatform(Platform platform) {
    this.platform = platform;
  }

  @Override
  public String buildQualifiedTopicName(Topic topic) {
    return topicNamingStrategy.buildTopicName(topic);
  }

  @Override
  public String buildProjectTopicPrefix(Project project) {
    return topicNamingStrategy.buildTopicPrefix(project);
  }

  public Platform getPlatform() {
    return this.platform;
  }

  public Boolean isEmpty() {
    return context.isEmpty();
  }

  @Override
  public Topology clone() {
    try {
      return (Topology) super.clone();
    } catch (CloneNotSupportedException e) {
      Topology topology = new TopologyImpl();
      topology.setContext(getContext());
      topology.setPlatform(getPlatform());
      topology.setProjects(getProjects());
      return topology;
    }
  }
}
