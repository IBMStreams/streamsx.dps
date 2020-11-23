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
# (C) Copyright IBM Corp. 2017  All Rights reserved.         
#                                                                  
# end_generated_IBM_copyright_prolog                               

# Since the DPS toolkit version you are using could be more
# recent than the one in the IBM Streams installation directory,
# you may want to ensure that the following DPS toolkit path is correct.
DPS_TOOLKIT_HOME ?= $(HOME)/streamsx.dps/com.ibm.streamsx.dps

SPLC_FLAGS ?= -a -t $(DPS_TOOLKIT_HOME)
SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_CMD_ARGS ?= --data-directory=./data 
SPL_MAIN_COMPOSITE = application::DpsTTLCompositesSample
OUTPUT_DIRECTORY = ./output/DpsTTLCompositesSample/Distributed

all: distributed

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) --output-directory=$(OUTPUT_DIRECTORY)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-directory=$(OUTPUT_DIRECTORY)

