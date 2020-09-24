package com.purbon.kafka.topology.model;

import java.util.LinkedHashMap;
import java.util.List;

public interface Topology {

  String getContext();

  void setContext(String context);

  List<Project> getProjects();

  void addProject(Project project);

  void setProjects(List<Project> projects);

  void addOther(String fieldName, String value);

  LinkedHashMap<String, String> otherNameComponents();

  void setPlatform(Platform platform);

  String buildQualifiedTopicName(Topic topic);

  String buildProjectTopicPrefix(Project project);

  Platform getPlatform();

  Boolean isEmpty();
}
