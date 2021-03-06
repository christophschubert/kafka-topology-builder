package com.purbon.kafka.topology;

import com.purbon.kafka.topology.clusterstate.RedisStateProcessor;
import com.purbon.kafka.topology.model.Topology;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.purbon.kafka.topology.BuilderCLI.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

public class KafkaTopologyBuilderTest {

  @Mock TopologyBuilderAdminClient topologyAdminClient;

  @Mock AccessControlProvider accessControlProvider;

  @Mock BindingsBuilderProvider bindingsBuilderProvider;

  @Mock TopicManager topicManager;

  @Mock AccessControlManager accessControlManager;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  private Map<String, String> cliOps;
  private Properties props;

  @Before
  public void before() {
    cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(ADMIN_CLIENT_CONFIG_OPTION, "/fooBar");

    props = new Properties();

    props.put(TopologyBuilderConfig.CONFLUENT_SCHEMA_REGISTRY_URL_CONFIG, "http://foo:8082");

    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "");
    props.put(AdminClientConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
  }

  @Test
  public void buildTopicNameTest() throws URISyntaxException, IOException {

    URL dirOfDescriptors = getClass().getResource("/dir");
    String fileOrDirPath = Paths.get(dirOfDescriptors.toURI()).toFile().toString();

    Topology topology = TopologyDescriptorBuilder.build(fileOrDirPath);

    assertEquals(4, topology.getProjects().size());
  }

  @Test
  public void closeAdminClientTest() throws URISyntaxException, IOException {
    URL dirOfDescriptors = getClass().getResource("/descriptor.yaml");
    String fileOrDirPath = Paths.get(dirOfDescriptors.toURI()).toFile().toString();

    TopologyBuilderConfig builderConfig = new TopologyBuilderConfig(cliOps, props);

    KafkaTopologyBuilder builder =
        KafkaTopologyBuilder.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider);

    builder.close();

    verify(topologyAdminClient, times(1)).close();
  }

  @Test(expected = IOException.class)
  public void verifyProblematicParametersTest() throws IOException {
    String file = "fileThatDoesNotExist.yaml";
    TopologyBuilderConfig builderConfig = new TopologyBuilderConfig(cliOps, props);

    KafkaTopologyBuilder builder =
        KafkaTopologyBuilder.build(
            file,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider);

    builder.verifyRequiredParameters(file, cliOps);
  }

  @Test
  public void verifyProblematicParametersTestOK() throws IOException, URISyntaxException {
    URL dirOfDescriptors = getClass().getResource("/descriptor.yaml");
    String fileOrDirPath = Paths.get(dirOfDescriptors.toURI()).toFile().toString();

    URL clientConfigURL = getClass().getResource("/client-config.properties");
    String clientConfigFile = Paths.get(clientConfigURL.toURI()).toFile().toString();

    cliOps.put(ADMIN_CLIENT_CONFIG_OPTION, clientConfigFile);

    TopologyBuilderConfig builderConfig = new TopologyBuilderConfig(cliOps, props);
    KafkaTopologyBuilder builder =
        KafkaTopologyBuilder.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider);

    builder.verifyRequiredParameters(fileOrDirPath, cliOps);
  }

  @Test
  public void builderRunTestAsFromCLI() throws URISyntaxException, IOException {

    URL dirOfDescriptors = getClass().getResource("/descriptor.yaml");
    String fileOrDirPath = Paths.get(dirOfDescriptors.toURI()).toFile().toString();

    URL clientConfigURL = getClass().getResource("/client-config.properties");
    String clientConfigFile = Paths.get(clientConfigURL.toURI()).toFile().toString();

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, "localhost:9092");
    config.put(ALLOW_DELETE_OPTION, "false");
    config.put(DRY_RUN_OPTION, "false");
    config.put(QUIET_OPTION, "false");
    config.put(ADMIN_CLIENT_CONFIG_OPTION, clientConfigFile);

    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put("confluent.schema.registry.url", "mock://");
    KafkaTopologyBuilder builder = KafkaTopologyBuilder.build(fileOrDirPath, config, properties);

    builder.setTopicManager(topicManager);
    builder.setAccessControlManager(accessControlManager);

    doNothing().when(topicManager).apply(anyObject(), anyObject());

    doNothing().when(accessControlManager).apply(anyObject(), anyObject());

    builder.run();
    builder.close();

    verify(topicManager, times(1)).apply(anyObject(), anyObject());
    verify(accessControlManager, times(1)).apply(anyObject(), anyObject());
  }

  @Mock RedisStateProcessor stateProcessor;

  @Test
  public void builderRunTestAsFromCLIWithARedisBackend() throws URISyntaxException, IOException {

    URL dirOfDescriptors = getClass().getResource("/descriptor.yaml");
    String fileOrDirPath = Paths.get(dirOfDescriptors.toURI()).toFile().toString();

    URL clientConfigURL = getClass().getResource("/client-config-redis.properties");
    String clientConfigFile = Paths.get(clientConfigURL.toURI()).toFile().toString();

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, "localhost:9092");
    config.put(ALLOW_DELETE_OPTION, "false");
    config.put(DRY_RUN_OPTION, "false");
    config.put(QUIET_OPTION, "false");
    config.put(ADMIN_CLIENT_CONFIG_OPTION, clientConfigFile);

    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put("confluent.schema.registry.url", "mock://");
    KafkaTopologyBuilder builder = KafkaTopologyBuilder.build(fileOrDirPath, config, properties);

    builder.setTopicManager(topicManager);
    builder.setAccessControlManager(accessControlManager);

    doNothing().when(topicManager).apply(anyObject(), anyObject());

    doNothing().when(accessControlManager).apply(anyObject(), anyObject());

    ClusterState cs = new ClusterState(stateProcessor);
    ExecutionPlan plan = ExecutionPlan.init(cs, System.out);

    builder.run(plan);
    builder.close();

    verify(stateProcessor, times(2)).createOrOpen();
    verify(topicManager, times(1)).apply(anyObject(), anyObject());
    verify(accessControlManager, times(1)).apply(anyObject(), anyObject());
  }

  @Test
  public void buiderRunTest() throws URISyntaxException, IOException {
    URL dirOfDescriptors = getClass().getResource("/descriptor.yaml");
    String fileOrDirPath = Paths.get(dirOfDescriptors.toURI()).toFile().toString();

    TopologyBuilderConfig builderConfig = new TopologyBuilderConfig(cliOps, props);

    KafkaTopologyBuilder builder =
        KafkaTopologyBuilder.build(
            fileOrDirPath,
            builderConfig,
            topologyAdminClient,
            accessControlProvider,
            bindingsBuilderProvider);

    builder.setTopicManager(topicManager);
    builder.setAccessControlManager(accessControlManager);

    doNothing().when(topicManager).apply(anyObject(), anyObject());

    doNothing().when(accessControlManager).apply(anyObject(), anyObject());

    builder.run();
    builder.close();

    verify(topicManager, times(1)).apply(anyObject(), anyObject());
    verify(accessControlManager, times(1)).apply(anyObject(), anyObject());
  }
}
