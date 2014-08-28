Setup of eclipse for the eguan project
--------------------------------------

1- Download and install eclipse

Download and install eclipse for Linux 64 bits, Luna.
If not present, install the JDT, the CDT, the EGit and the m2e
plugins.

2- Configuration of the text editors

Menu Windows / Preferences

2.1 General
 Panel General / Editors / Text Editors
 - Check 'Insert spaces for tabs'
 Apply

2.2 XML
 Panel XML / XML Files / Editor
 - Set 'Line width' = 120
 - Check 'Indent using spaces'
 - Set 'Indentation size' = 4
 Apply

2.3 Java
 Panel Java / Code Style / Formatter
 Import the file 'eguan-java-formatter.xml'
 Apply

 Panel Java / Code Style / Clean Up
 Import the file 'eguan-java-cleanup.xml'
 Apply

2.4 C/C++
 Panel C/C++ / Code Style
 Import the file 'eguan-c-formatter.xml'
 Apply

3- Clone the repository and/or import projects

Before importing the projects, you should build the whole project to
install all the modules and build some generated code.
Eclipse may not find the generated code: you could close the projects
*-proto to avoid these errors.
