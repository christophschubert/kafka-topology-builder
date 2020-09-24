package com.purbon.kafka.topology;

import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopicImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TopicTest {

  Topology topology;
  Project project;

  @Before
  public void before() {
    topology = new TopologyImpl();
    topology.setContext("team");

    project = new ProjectImpl();
    project.setTopology(topology);
    // project.setTopologyPrefix(topology.buildNamePrefix());

    project.setName("project");
    topology.setProjects(Arrays.asList(project));
  }

  @Test
  public void buildTopicNameTest() {
    Topic topic = new TopicImpl("topic");
    topic.setProject(project);
    // topic.setProjectPrefix(project.buildTopicPrefix());
    String fullName = topic.toString();
    Assert.assertEquals("team.project.topic", fullName);
  }

  @Test
  public void buildTopicNameWithOtherDataPointsTest() {

    Topology topology = new TopologyImpl();
    topology.setContext("team");

    topology.addOther("other-f", "other");
    topology.addOther("another-f", "another");

    Project project = new ProjectImpl();
    project.setTopology(topology);
    // project.setTopologyPrefix(topology.buildNamePrefix());

    project.setName("project");
    topology.setProjects(Arrays.asList(project));

    Topic topic = new TopicImpl("topic");
    topic.setProject(project);
    // topic.setProjectPrefix(project.buildTopicPrefix());
    String fullName = topic.toString();
    Assert.assertEquals("team.other.another.project.topic", fullName);
  }

  @Test
  public void buildTopicNameWithDataTypeTest() {
    Topic topic = new TopicImpl("topic", "type");
    topic.setProject(project);
    // topic.setProjectPrefix(project.buildTopicPrefix());
    String fullName = topic.toString();
    Assert.assertEquals("team.project.topic.type", fullName);
  }
}
