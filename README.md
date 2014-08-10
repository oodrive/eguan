Eguan is a replicated storage system written in Java and C/C++.

It was implemented by [Oodrive](http://www.oodrive.com/) as a storage prototype for the project [nu@ge](http://nuage-france.fr). Nu@ge is the project of a french consortium aimed at developing a green cloud platform based on open source software and small modular data centers.

Eguan provides storage volumes to clients via an iSCSI or NBD connection and manages the history of this volumes. Each volume is composed of devices and snapshots, replicated on several nodes.

The system uses the strategy of copy-on-write for the snapshots and data deduplication to improve storage utilization.

## Quick build

To build the eguan server, you need to install:
- Java JDK 7
- gcc version 4.8 (or more) and the build essential tools for the C and C++ code
- [Maven](http://maven.apache.org) version 3.0.4 or more

First build the tools, go in the directory 'tools':

    ./install

Then the project can be built quickly (without unit tests execution) in the root directory:

    mvn clean ; mvn -DskipTests -DskipNarTests -DskipNarJniTests -PskipNarTestsCompile install

by default the project build on linux to build on osx add -Posx option

For more information, see the build page.

## Configuration and installation

You should have 2 kinds of local storage on your server:
* fast device for the temporary storage.
* high capacity device for the persistence.

Edit the config file vost.cfg (or cinost.cfg) and fill the necessary fields (see Installation and configuration page for more information)

To install the application:

    ./bin/install -d /data/vold/
 
Note: /data/vold/ is a directory contained in the high capacity volume.

To run the application:

    sudo /etc/init.d/vost start

