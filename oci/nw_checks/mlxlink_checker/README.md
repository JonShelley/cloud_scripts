# Initial Setup
Create a python virtual environment
cd <path_to_cloud_scripts>/oci/nw
- python3 -m venv venv
- source venv/bin/activate

Install the neccessary python modules
- pip3 install pandas numpy natsort Pyarrow tabulate

Generate host file with one host per line

Run Script
```
usage: run_mlxlink_info.py [-h] [--hostfile HOSTFILE] [-f EXE_FILE] [--script_directory SCRIPT_DIRECTORY] [-s] [-d] [-e] [-c] [-u USER] [--date_stamp DATE_STAMP] [--nfs] [--venv VENV] [--ber_threshold BER_THRESHOLD]
                           [--eff_threshold EFF_THRESHOLD] [--max_workers MAX_WORKERS] [-p PORT] [-w] [--flap_duration_threshold FLAP_DURATION_THRESHOLD]

Process some integers.

options:
  -h, --help            show this help message and exit
  --hostfile HOSTFILE   the hostfile name
  -f EXE_FILE, --exe_file EXE_FILE
                        the executable file
  --script_directory SCRIPT_DIRECTORY
                        the script directory
  -s, --setup_host      setup the host to run mlxlink_info
  -d, --distribute      distribute the executable file to the remote hosts
  -e, --execute         execute the executable file on the remote hosts
  -c, --collect         collect the results from the remote hosts
  -u USER, --user USER  the user name
  --date_stamp DATE_STAMP
                        the date stamp
  --nfs                 script directory is NFS mounted (default: False)
  --venv VENV           specify the python virtual environment to use
  --ber_threshold BER_THRESHOLD
                        specify the BER threshold
  --eff_threshold EFF_THRESHOLD
                        specify the BER threshold
  --max_workers MAX_WORKERS
                        specify the maximum number of workers (default: 32)
  -p PORT, --port PORT  specify the ssh port number (default: 22)
  -w, --warning         enable warning messages
  --flap_duration_threshold FLAP_DURATION_THRESHOLD
                        specify the link flap duration threshold in hours(default: 12)
```
Example: Run the script. Check for link flaps in the past 48 hours. Only flag links with more that 100K effective physical errors. Use the python virtual environment found at the specified location
```
python3 run_mlxlink_info.py --hostfile hostlist.txt --exe_file mlxlink_info.py --script_directory /app/sce/cloud_scripts/oci/nw_checks/mlxlink_checker -e --eff_threshold 100000 --flap_duration_threshold 172800 --venv /app/sce/cloud_scripts/oci/nw_checks/mlxlink_checker/venv
```

## If you only want to collect the data that mlxlink_info.py collect and put it in a file for later review then you can run mlxlink_info_min.py. This will generate a json file which includes the hostname.
```
python3 mlxlink_info_min.py
```
