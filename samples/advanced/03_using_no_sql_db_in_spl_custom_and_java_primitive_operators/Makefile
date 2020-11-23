# begin_generated_IBM_copyright_prolog                             
#                                                                  
# This is an automatically generated copyright prolog.             
# After initializing,  DO NOT MODIFY OR MOVE                       
# **************************************************************** 
# THIS SAMPLE CODE IS PROVIDED ON AN "AS IS" BASIS. IBM MAKES NO   
# REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED, CONCERNING    
# USE OF THE SAMPLE CODE, OR THE COMPLETENESS OR ACCURACY OF THE   
# SAMPLE CODE. IBM DOES NOT WARRANT UNINTERRUPTED OR ERROR-FREE    
# OPERATION OF THIS SAMPLE CODE. IBM IS NOT RESPONSIBLE FOR THE    
# RESULTS OBTAINED FROM THE USE OF THE SAMPLE CODE OR ANY PORTION  
# OF THIS SAMPLE CODE.                                             
#                                                                  
# LIMITATION OF LIABILITY. IN NO EVENT WILL IBM BE LIABLE TO ANY   
# PARTY FOR ANY DIRECT, INDIRECT, SPECIAL OR OTHER CONSEQUENTIAL   
# DAMAGES FOR ANY USE OF THIS SAMPLE CODE, THE USE OF CODE FROM    
# THIS [ SAMPLE PACKAGE,] INCLUDING, WITHOUT LIMITATION, ANY LOST  
# PROFITS, BUSINESS INTERRUPTION, LOSS OF PROGRAMS OR OTHER DATA   
# ON YOUR INFORMATION HANDLING SYSTEM OR OTHERWISE.                
#                                                                  
# (C) Copyright IBM Corp. 2016  All Rights reserved.         
#                                                                  
# end_generated_IBM_copyright_prolog                               

# Since the DPS toolkit version you are using could be more
# recent than the one in the IBM Streams installation directory,
# you may want to ensure that the following DPS toolkit path is correct.
DPS_TOOLKIT_HOME ?= $(HOME)/streamsx.dps/com.ibm.streamsx.dps

# The dps-helper is copied from the DPS toolkit directory to the 
# impl/java/lib directory of this application to ensure it is 
# included in the SAB file.
DPS_HELPER_JAR = $(DPS_TOOLKIT_HOME)/impl/java/lib/dps-helper.jar

# If the user wants to use a different version of
# the DPS toolkit than the one shipped in the
# $STREAMS_INSTALL/toolkits directory, (s)he can
# always set the STREAMS_SPLPATH environment variable
# and point to a specific version of the DPS toolkit.
#
# IMPORTANT 
# ---------
# If the user wants to point to a different DPS toolkit directory 
# than the one shipped in the IBM Streams installation directory,
# then it is necessary to make changes in the following
# three files present in this project directory as well 
# before running this Makefile.
# 1) build.xml in the same directory as this Makefile.
#    --> In that file, search for dps-helper.jar and change its full path to
#        point to the new DPS toolkit directory being used.
#
# 2) impl/java/src/com/acme/test/TickerIdGenerator.java
#    --> Search for @Libraries, read the commentary there to copy
# 	 the dps-helper.jar file locally within this application and
#	 activate the correct @Libraries annotation as explained there.
#
# 3) impl/java/src/com/ibm/acme/test/DataStoreTest.java
#    --> Search for @Libraries, read the commentary there to copy
# 	 the dps-helper.jar file locally within this application and
#	 activate the correct @Libraries annotation as explained there.
# 
# This example is already set up correctly with the 3 steps mentioned above.
#
ifndef STREAMS_SPLPATH
   SPLC_FLAGS ?= -a -z -t $(DPS_TOOLKIT_HOME)
else
   SPLC_FLAGS ?= -a -z 
endif

SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= --data-directory=./data 
SPL_MAIN_COMPOSITE = com.acme.test::Main
JAVA_CLASS_FILES = impl/java/src/com/ibm/acme/test/DataStoreTester.java impl/java/src/com/acme/test/TickerIdGenerator.java

all: distributed

distributed: java
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --output-directory=./output/com.acme.test.Main/Distributed

java: $(JAVA_CLASS_FILES)  
	mkdir -p impl/java/lib
	cp $(DPS_HELPER_JAR) impl/java/lib
	JAVA_HOME=$(STREAMS_INSTALL)/java ant

clean: 
	ant clean
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE)
	rm -rf output
