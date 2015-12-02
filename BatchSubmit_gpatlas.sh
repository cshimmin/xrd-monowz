#!/bin/bash

show_help() {
    echo "Driver to run the CxAODReader jobs either locally or as a batch job."
    echo "Usage: $0 [options]"
    echo "  -c CONFIG_FILE   Path to the job config file"
    echo "  -f FILE_LIST     Path to the input file list"
    echo "  -s SAMPLE        Sample name"
    echo "  -o OUTPUT_PATH   Destination path to which results are copied"
}

while getopts ":h?c:f:s:o:" opt; do
    case "$opt" in
    h) show_help
       exit 0
       ;;
    c) config_file=$OPTARG
       ;;
    f) file_list=$OPTARG
       ;;
    s) sample_name=$OPTARG
       ;;
    o) output_path=$OPTARG
       ;;
   \?) echo "Invalid option: -$OPTARG" >&2
       show_help
       exit 1
       ;;
    :) echo "Option: -$OPTARG requires argument." >&2
       show_help
       exit 1
       ;;
    esac
done

# check arguments
if [ -z "$config_file" ]; then
    echo "No config file specified!" >&2
    show_help
    exit 1
fi

if [ -z "$file_list" ]; then
    echo "No file list specified!" >&2
    show_help
    exit 1
fi

if [ -z "$sample_name" ]; then
    echo "No sample name specified!" >&2
    show_help
    exit 1
fi

if [ -z "$output_path" ]; then
    echo "No output path specified!" >&2
    show_help
    exit 1
fi


# check if we're running as a SLURM job:
if [ ! -z "$SLURM_JOB_ID" ]; then
    echo "Running as batch job on host: $(hostname)"

    workdir=/local/scratch/$(whoami)/monoV_${SLURM_JOB_ID}
    echo "Will use local scratch path: $workdir"
else
    echo "No SLURM environment detected. Will run in local mode."
    workdir=$(mktemp -d)
    echo "Will use local tmp path: $workdir"
fi

clean_stage() {
    echo "Removing work directory: $workdir"
    rm -rf $workdir
}

mkdir -p $workdir

cp $file_list $workdir/files.list

pushd $workdir

# probably not necessary if it was setup before submitting?
#echo "SETUP - rcSetup"
#source rcSetup.sh

echo "RUNNING - main executable"
hsg5frameworkReadCxAOD_monoVH submit_dir $config_file $sample_name filelist files.list
code=$?

if [ $code -ne 0 ]; then
    echo "The CxAOD reader returned an error!" >&2
    clean_stage
    exit $code
fi

popd

echo "FINALIZE - copying output to local space"
mkdir -p $output_path

cp -r ${workdir}/submit_dir ${output_path}/${sample_name}
code=$?
if [ $code -ne 0 ]; then
    echo "Failed to copy outputs!"
    clean_stage
    exit $code
fi

echo "Done!"
clean_stage
