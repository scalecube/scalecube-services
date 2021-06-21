package io.scalecube.services.examples.docker;

import static io.scalecube.services.examples.docker.ExampleServiceRunner.DEFAULT_DISCOVERY_PORT;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.compress.utils.IOUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import reactor.core.Exceptions;

public class ExampleServiceRunnaerViaDockerTest {

  public static final String SEED_ALIAS = "seed";
  public static final String SERVICE_NODE_ALIAS = "serviceNode";

  public static void main(String[] args) throws Exception {
    ImageFromDockerfile image = createImage();

    GenericContainer seed =
        new GenericContainer<>(image)
            .withExposedPorts(DEFAULT_DISCOVERY_PORT)
            .withEnv("logLevel", "DEBUG")
            .withNetwork(Network.SHARED)
            .withNetworkAliases(SEED_ALIAS)
            .withCreateContainerCmdModifier(cmd -> cmd.withName(SEED_ALIAS))
            .waitingFor(new HostPortWaitStrategy());

    seed.start();

    GenericContainer serviceNode =
        new GenericContainer<>(image)
            .withExposedPorts(DEFAULT_DISCOVERY_PORT)
            .withEnv("JAVA_OPTS", "-Dseeds=seed:" + DEFAULT_DISCOVERY_PORT)
            .withEnv("logLevel", "DEBUG")
            .withNetwork(Network.SHARED)
            .withNetworkAliases(SERVICE_NODE_ALIAS)
            .withCreateContainerCmdModifier(cmd -> cmd.withName(SERVICE_NODE_ALIAS))
            .waitingFor(new HostPortWaitStrategy());

    serviceNode.start();

    Thread.currentThread().join();
  }

  private static ImageFromDockerfile createImage() throws IOException {
    File targetJar = findTargetJar();
    return new ImageFromDockerfile()
        .withFileFromFile("services-examples/target/tests.jar", targetJar)
        .withFileFromFile("services-examples/target/lib", new File("services-examples/target/lib"))
        .withFileFromClasspath("Dockerfile", "TestServiceRunnerDockerfile");
  }

  private static File findTargetJar() throws IOException {
    return Files.list(Paths.get("services-examples", "target"))
        .filter(p -> p.toString().endsWith("tests.jar"))
        .findFirst()
        .map(Path::toFile)
        .filter(File::exists)
        .orElseGet(
            () -> {
              try {
                Process process =
                    Runtime.getRuntime()
                        .exec("mvn package -DskipTests -P SkipDockerGoals,!RunDockerGoals");
                IOUtils.copy(process.getInputStream(), System.out);

                return Files.list(Paths.get("services-examples", "target"))
                    .filter(p -> p.toString().endsWith("tests.jar"))
                    .findFirst()
                    .map(Path::toFile)
                    .filter(File::exists)
                    .orElseThrow(IllegalStateException::new);
              } catch (Exception e) {
                throw Exceptions.propagate(e);
              }
            });
  }
}
