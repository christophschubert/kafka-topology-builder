package com.purbon.kafka.topology.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Topology;
import java.io.File;
import java.io.IOException;

public class TopologySerdes {

  ObjectMapper mapper;

  public TopologySerdes() {
    mapper = new ObjectMapper(new YAMLFactory());
    SimpleModule module = new SimpleModule();
    module.addDeserializer(Topology.class, new TopologyCustomDeserializer());
    module.addDeserializer(ProjectImpl.class, new ProjectCustomDeserializer());
    mapper.registerModule(module);
    mapper.registerModule(new Jdk8Module());
    mapper.findAndRegisterModules();
  }

  public Topology deserialise(File file) throws IOException {
    return mapper.readValue(file, Topology.class);
  }

  public Topology deserialise(String content) throws IOException {
    return mapper.readValue(content, Topology.class);
  }

  public String serialise(Topology topology) throws JsonProcessingException {
    return mapper.writeValueAsString(topology);
  }
}
