# Initial Setup
Create a python virtual environment
cd <path_to_cloud_scripts>/oci/nw
- python3 -m venv venv
- source venv/bin/activate

Install the neccessary python modules
- pip3 install pandas numpy natsort Pyarrow tabulate

Generate host file with one host per line

Run Script
python3 run_mlxlink_info.py --hostfile <hostfile_to_test> --exe_file mlxlink_info.py --script_directory <path to cloud_scripts dir>/oci/nw_checks/mlxlink_checker -p <ssh port> -e --user <user name> --eff_threshold <effective error threshold> --flap_duration_threshold <time in secondes. I recommend 172800>

Example: Run the script. Check for link flaps in the past 48 hours. Only flag links with more that 100K effective physical errors. Use the python virtual environment found at the specified location

python3 run_mlxlink_info.py --hostfile hostlist.txt --exe_file mlxlink_info.py --script_directory /app/sce/cloud_scripts/oci/nw_checks/mlxlink_checker -e --eff_threshold 100000 --flap_duration_threshold 172800 --venv /app/sce/cloud_scripts/oci/nw_checks/mlxlink_checker/venv
