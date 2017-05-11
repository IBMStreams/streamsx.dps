#ifndef FUNCTIONS_H_
#define FUNCTIONS_H_

/*
=================================================================================================
**** TIPS ABOUT CALLING THE dps APIs FROM INSIDE THE USER-WRITTEN NATIVE FUNCTIONS ****

In this example, our goal is to use the dps native function APIs directly
inside a user-written native function.

1) Refer to the ../../impl/bin/archLevel file that we created in this example directory.
(A common practice is to copy an existing archLevel file and modify it to suit your project's needs.)
In that file, search for com.ibm.streamsx.dps and you will see in two different places where we
have set the path to point to the impl/lib and impl/include directories belonging to the
dps toolkit present in your $STREAMS_INSTALL directory. 

2) In the native function model XML file of this SPL project, the archLevel 
   file referred to above is being used to automatically add the DPS include 
   directory and the required  K/V store third party libraries in the function model XML file.
   That XML file is located in this <Project_Directory>/com.acme.test/native.function/function.xml.

3) We have to include the dps wrapper include file in this native function include file.
   [See the first non-commented line below this comment block.]

4) Finally, we will add "using namespace" of the C++ implementation for the
dps into this user-written native function's scope. That will allow us to use any of the
dps's public member functions within this native function code.

That is it. Now, you can call any of the dps native function APIs inside this
user-written native functions.
=================================================================================================
*/

// Include this SPL file so that we can use the SPL functions and types in this C++ code.
#include "DistributedProcessStoreWrappers.h"
#include "SPL/Runtime/Function/SPLFunctions.h"
#include <vector>
#include <sstream>
#include <iostream>
#include <cstdio>
#include <cstdlib>

// Define a C++ namespace that will contain our native function code.
namespace calling_dps_apis_from_a_native_function {
	// Bring the dps namespace here.
	using namespace com::ibm::streamsx::store::distributed;
	using namespace std;
	// By including this line, we will have access to the SPL namespace and anything defined within that.
	using namespace SPL;

		// Prototype for our native functions are declared here.
		void generateTickerIds(SPL::list<rstring> listOfStockPicks);

		// Generate ticker ids.
		// What is the simple business logic being performed here?
		// This native will pull out the "ticker symbol and the company name" from Thing1_Store ONLY for
		// those ticker symbols sent in the stock picks list as an input argument to this native function.
		// Then, it will create a unique ticker id for every ticker symbol specified in the stock picks list and insert
		// "ticker symbol => unique ticker id" in a new store called "Thing2_Store".
		inline void generateTickerIds(SPL::list<rstring> listOfStockPicks)
		{
			// Let us get the store id for the "Thing1 store", where the complete set of
			// "ticker symbol to company name" mappings are kept.
			uint64 e1 = 0;
			uint64 & err = static_cast<uint64 &> (e1);
			// Pass a dummy key and dummy value as method arguments.
			rstring dummyRstring = "";
		    uint64 t1s = dpsCreateOrGetStore("Thing1_Store", dummyRstring, dummyRstring, err);

		    if (t1s == 0) {
		    	cout << "Functions.h-->generateTickerIds: Unable to get the store id for Thing1_Store." << endl;
		    	SPL::Functions::Utility::abort("Functions.h", 72);
		    }

		    // Let us create a brand new store called "Thing2_Store".
		    uint64 t2s = dpsCreateOrGetStore("Thing2_Store", dummyRstring, dummyRstring, err);

		    if (t2s == 0) {
		    	cout << "Functions.h-->generateTickerIds: Unable to create a new store called Thing2_Store." << endl;
		    	SPL::Functions::Utility::abort("Functions.h", 77);
		    }

		    int32 listSize = SPL::Functions::Collections::size(listOfStockPicks);
		    // Now loop through the stock picks list.
			for (int32 cnt=0; cnt<listSize; cnt++) {
		    	rstring tickerSymbol = listOfStockPicks[cnt];
		    	rstring cn = "";
		    	rstring & companyName = static_cast<rstring &> (cn);
		    	// Look up the company name for a given ticker symbol from the "Thing1_Store".
		    	boolean res = false;
		    	res = dpsGet(t1s, tickerSymbol, companyName, err);

		    	if (res == false) {
		    		cout << "Functions.h-->generateTickerIds: Unable to get the company name from Thing1_Store for ticker symbol " << tickerSymbol << "." << endl;
		    		SPL::Functions::Utility::abort("Functions.h", 92);
		    	}

		    	// Let us compute a unique hash code for this company name.
		    	uint64 tickerId = companyName.hashCode();
		    	// Put this away in the Thing2_Store now.
		    	res = dpsPut(t2s, tickerSymbol, tickerId, err);

		    	if (res == false) {
		    		cout << "Functions.h-->generateTickerIds: Unable to put the tickerId " << tickerId << " for the ticker symbol " <<
		    			tickerSymbol << " in the Thing2_Store." << endl;
		    		SPL::Functions::Utility::abort("Functions.h", 103);
		    	}
		    }

		}

}
#endif
