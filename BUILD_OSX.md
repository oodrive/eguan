This is a quick description for OSX build of eguan project.

## Quick build

To build the eguan server, you need to install:
- Apple XCode tools suite (download and install from https://developer.apple.com/xcode/downloads/)
- Java JDK 7 (download and install from http://www.oracle.com/technetwork/java/index.html)
- Homebrew (download and install from http://brew.sh)
- gcc version 4.8 (or more) and the build essential tools for the C and C++ code (install details below)
- [Maven](http://maven.apache.org) version 3.0.4 or more (install details below)

Before trying to build, make sure homebrew installation is working and up to date by launching:
    brew doctor && brew update && brew upgrade && brew cleanup

If everything is OK, launch the following homebrew command. It may take about 1h or more to compile:
    brew install maven autoconf automake libtool pkg-config gcc48 


Then build the tools for eguan, go in the directory 'tools' and do:
    ./install

The project can now be built (without unit tests execution) in the root directory:
    mvn clean ; mvn -DskipTests -DskipNarTests -DskipNarJniTests -PskipNarTestsCompile -Posx install

## Troubleshooting

Some errors that you can encounter and solutions :
- maven can't find JAVA, check .zshrc (or .bashrc ...) file it should have something like this :
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_45.jdk/Contents/Home/
    export PATH=$JAVA_HOME/bin:$PATH
    export LD_LIBRARY_PATH=$JAVA_HOME/lib:$LD_LIBRARY_PATH

- conflict with system binaries, check .zshrc file and add something like this:
    export PATH=/usr/local/bin:$PATH
    export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

- problem with uuid: you may need to install e2fsprogs with homebrew
    brew install e2fsprogs

For more information, see the linux build page.
