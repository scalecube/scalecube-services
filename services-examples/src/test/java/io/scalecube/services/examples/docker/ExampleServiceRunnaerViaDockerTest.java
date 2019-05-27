package io.scalecube.services.examples.docker;

import com.github.dockerjava.api.model.PortBinding;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.compress.utils.IOUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import reactor.core.Exceptions;

public class ExampleServiceRunnaerViaDockerTest {

  public static void main(String[] args) throws Exception {

    File targetJar = getTargetJar();

    ImageFromDockerfile image =
        new ImageFromDockerfile()
            .withFileFromFile("services-examples/target/tests.jar", targetJar)
            .withFileFromFile(
                "services-examples/target/lib", new File("services-examples/target/lib"))
            .withFileFromClasspath("Dockerfile", "TestServiceRunnerDockerfile");

    GenericContainer container =
        new GenericContainer<>(image)
            .withExposedPorts(4801)
            .withEnv("JAVA_OPTS", "-Dseeds=localhost:4801 -Ddiscovery.port=4801 -Dmember.host=seed")
            //        .withEnv("MEMBER_HOST", "4801")
            //        .withEnv("MEMBER_PORT", "4801")
            .withEnv("logLevel", "DEBUG")
            .withNetwork(Network.SHARED)
            .withNetworkAliases("seed")
            .withCreateContainerCmdModifier(
                cmd -> {
                  cmd.withName("seed").withPortBindings(PortBinding.parse(4801 + ":" + 4801));
                })
        //            .waitingFor(new HostPortWaitStrategy())
        ;

    container.start();

    GenericContainer container2 =
        new GenericContainer<>(image)
            .withExposedPorts(4802)
            .withEnv(
                "JAVA_OPTS", "-Dseeds=localhost:4802 -Ddiscovery.port=4802 -Dmember.host=follower")
            //        .withEnv("MEMBER_HOST", "4801")
            //        .withEnv("MEMBER_PORT", "4801")
            .withEnv("logLevel", "DEBUG")
            .withNetwork(Network.SHARED)
            .withNetworkAliases("follower")
            .withCreateContainerCmdModifier(
                cmd -> {
                  cmd.withName("follower").withPortBindings(PortBinding.parse(4802 + ":" + 4802));
                })
        //            .waitingFor(new HostPortWaitStrategy())
        ;

    container2.start();

    Thread.currentThread().join();
  }

  private static File getTargetJar() throws IOException {
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
