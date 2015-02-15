s=0	# Number of options to shift out
while getopts "i:p:o:r:f:s:n:h" options; do
    case $options in
    i) streams_instance_name=$OPTARG
       let s=s+2
       ;;

    h | * ) echo "
Command line arguments
  -i STRING     streamsInstanceName                       		(silver_stream)
 
  e-g:
  -i silver_stream
"
        exit 1
        ;;
    esac
done
shift $s

# Validate the streams instance name entered by the user.
if [ "$streams_instance_name" == "" ];
then
   echo "Missing or wrong streams instance name via the -i option."
   echo "Your streams instance name must be specified."
   echo ""
   echo "Get help using -h option."
   exit 1
fi


# Now, we can go ahead and start the instance (if not already running) and then
# start the dps test application.
echo "Ensuring that the Streams instance '$streams_instance_name' is running ..."
# If Streams instance is already started and running, it will display a warning on
# the stderr console. Let us suppress that warning by redirecting stderr and 
# stdout to a null device.
streamtool startinstance -i $streams_instance_name &> /dev/null

streamtool submitjob -i $streams_instance_name ../output/DpsTest1/Distributed/DpsTest1.adl

echo "****** You can check the results from this run inside the PE stdouterr log files in the Streams application log directory.  ******"
