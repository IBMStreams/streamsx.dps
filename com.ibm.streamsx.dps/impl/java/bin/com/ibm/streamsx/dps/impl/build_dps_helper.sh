#!/bin/sh
#==================================================================
# Add -x at the end of the first line of this file to debug this script.
#
# This script compiles the DpsHelper java file and many other 
# classes that are part of an OO layer surrounding the DpsHelper class.
# Then, it creates a jar file. It moves the jar file to impl/java/bin directory
# of the dps toolkit. It also moves the newly built java class file along with
# its full package directory to that same location.
#
# As part of building the java file, this script also generates the
# JNI C++ header file. It moves that header file
# into the impl/include directory of the toolkit. Whenever a new
# "private native ..." statement is added in the DpsHelper Java file, it is 
# necessary to provide a C++ implementation for that new native 
# method in our impl/src/com_ibm_streamsx_dps_DpsHelper.cpp file.
#
# In order for this to work, you must ensure that these two Linux environment
# variables are set correctly:  $STREAMS_INSTALL and $JAVA_HOME
# 
# High-level development steps required for this JNI work:
#
# 1) Compile this DpsHelper and its OO layer Java classes.
#
# 2) After compiling it, generate a native method C++ header file using javah command.
#
# 3) Write the JNI C++ code and build a .so library out of that CPP file.
#    [Refer to the impl/mk script in this dps toolkit.]
#
# 4) Specify the correct library path(s) in your Java primitive operator model XML file.
#
# 5) Refer to a working example that shows how to use the dps functions inside a
#    Java primitive operator.
#    062_data_sharing_between_non_fused_spl_custom_and_java_primitive_operators from this
#    IBM dW Streams Exchange URL: http://tinyurl.com/3apa97q
#==================================================================	
echo "============================================================================="
date
rm -f ./*.class
rm -f ../*.class
rm -rf ../com

# Following java command may give this warning: Recompile with -Xlint:unchecked for details. [You can proceed further by ignoring it.]
# Change to the parent directory of this java class package i.e. impl/java/src directory.
cd ../../../../../
javac -cp $STREAMS_INSTALL/lib/com.ibm.streams.operator.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar:$STREAMS_INSTALL/lib/commons-math-2.2.jar:$CLASSPATH ./com/ibm/streamsx/dps/*.java || { echo "Error while building the DpsHelper and other classes in its OO layer." ; exit 1; }
cd ./com/ibm/streamsx/dps
# We can move the .class files to their correct package directory.
mkdir -p ./com/ibm/streamsx/dps/impl
mv ./*.class ./com/ibm/streamsx/dps
mv ./impl/*.class ./com/ibm/streamsx/dps/impl
# Let us create a native method C++ header file.
javah com.ibm.streamsx.dps.impl.DpsHelper
mv *.h ../../../../../../include

# Let us now build the distributed locking Java files.
# Change to the parent directory of this java class package i.e. impl/java/src directory.
cd ../../../../
javac -cp $STREAMS_INSTALL/lib/com.ibm.streams.operator.jar:$STREAMS_INSTALL/lib/com.ibm.streams.operator.samples.jar:$STREAMS_INSTALL/lib/commons-math-2.2.jar:$CLASSPATH ./com/ibm/streamsx/dl/*.java || { echo "Error while building the Distributed Lock Java files." ; exit 1; }
cd ./com/ibm/streamsx/dl
# We can move the .class files to their correct package directory.
mkdir -p ../dps/com/ibm/streamsx/dl/impl
mv ./*.class ../dps/com/ibm/streamsx/dl
mv ./impl/*.class ../dps/com/ibm/streamsx/dl/impl
cd ../dps

# Let us create a jar file for the newly created class files.
jar -cvfM dps-helper.jar ./com/ibm/streamsx/dps/*.class ./com/ibm/streamsx/dps/impl/*.class  ./com/ibm/streamsx/dl/*.class ./com/ibm/streamsx/dl/impl/*.class
# Move the jar file and the entire com/ibm/streamsx/...... directory to the impl/java/bin directory.
rm -f ../../../../../bin/dps-helper.jar
mv dps-helper.jar ../../../../../bin
rm -rf ./com
# Sometimes, I noticed a copy of the .class files left behind in the impl directory. Delete them now.
rm -f ./impl/*.class
echo "DpsHelper and other classes in its OO layer were built successfully."
date
echo "============================================================================="
