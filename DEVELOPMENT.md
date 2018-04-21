# Setting up development environment

## Install the necessary build tools
You require to have latest stable [Oracle JDK 7](http://java.oracle.com/), latest stable 
[Apache Maven](http://maven.apache.org/) and [Git](http://git-scm.com/) installed on your machine.

## Set up IntelliJ IDEA
ScaleCube project team uses [IntelliJ IDEA](http://www.jetbrains.com/idea/) as the primary IDE, although we are fine 
with using other development environments as long as you adhere to our coding style.

### Code style
ScaleCube project team uses [Google code style](http://google.github.io/styleguide/javaguide.html) with 
next modifications:
* Maximum line length is 120
* Special package in import order is io.scalecube.

We use [Eclipse code formatter plugin] (https://github.com/krasa/EclipseCodeFormatter#instructions).
Download [this code style configuration](https://github.com/scalecube/scalecube/blob/master/eclipse-java-google-style.xml) 
and [import order configuration](https://github.com/scalecube/scalecube/blob/master/style.importorder) and configure 
your IDEA plugin with those settings. Configuration is  also available in the root project folder.
