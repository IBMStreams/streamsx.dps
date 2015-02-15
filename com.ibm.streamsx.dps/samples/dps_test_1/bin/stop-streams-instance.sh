#!/bin/sh
# ========================
# Use this script to stop your streams instance.
# ========================
s=0	# Number of options to shift out
while getopts "i:h" options; do
    case $options in
    i) streams_instance_name=$OPTARG
       let s=s+2
       ;;
    h | * ) echo "
Command line arguments
  -i STRING     streamsInstanceName (silver_stream)

  e-g:
  -i silver_stream
"
        exit 1
        ;;
    esac
done
shift $s

# Validate the streams instance name  entered by the user.
if [ "$streams_instance_name" == "" ];
then
   echo "Missing or wrong streams instance name via the -i option."
   echo "Your streams instance name must be specified."
   echo ""
   echo "Get help using -h option."
   exit 1
fi

# Let us stop the instance now.
streamtool stopinstance -i $streams_instance_name

date
