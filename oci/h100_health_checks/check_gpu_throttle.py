import subprocess
import os
import logging

logging.basicConfig(level=logging.info)

# Clocks Throttle Reason Codes: https://docs.nvidia.com/deploy/nvml-api/group__nvmlClocksThrottleReasons.html

GPU_THROTTLE_QUERY = "clocks_event_reasons.active"
GPU_CLOCKS_THROTTLE_REASON = {
   '0x0000000000000008': 'HW_SLOWDOWN',
   '0x000000000000004': 'HW_THERMAL_SLOWDOWN',
   '0x0000000000000002': 'APPLICATIONS_CLOCK_SETTINGS',
   '0x0000000000000100': 'DISPLAY_SETTINGS',
   '0x0000000000000001': 'GPU_IDLE',
   '0x0000000000000080': 'POWER_BRAKE_SLOWDOWN',
   '0x0000000000000000': 'NONE',
   '0x0000000000000004': 'SW_POWER_CAP',
   '0x0000000000000020': 'SW_THERMAL_SLOWDOWN',
   '0x0000000000000010': 'SYNC_BOOST'
}

def gather_gpu_clock_throttle_data():
   gpu_clock_throttle_query_out = subprocess.check_output(['nvidia-smi', '--query-gpu=' + GPU_THROTTLE_QUERY, '--format=csv,noheader,nounits']).decode().strip()
   gpu_clock_throttle_out_lines = gpu_clock_throttle_query_out.split('\n')
   return gpu_clock_throttle_out_lines

def check_gpu_clock_throttling():
   gpu_clock_throttle_out_lines = gather_gpu_clock_throttle_data()
   for i, line in enumerate(gpu_clock_throttle_out_lines):
      gpu_clock_throttle_out_line = line.split(', ')
      if gpu_clock_throttle_out_line[0] != GPU_CLOCKS_THROTTLE_REASON['0x0000000000000001'] and \
         gpu_clock_throttle_out_line[0] != GPU_CLOCKS_THROTTLE_REASON['0x0000000000000000'] \
         and gpu_clock_throttle_out_line[0] != GPU_CLOCKS_THROTTLE_REASON['0x0000000000000004']:
         if gpu_clock_throttle_out_line[0] in GPU_CLOCKS_THROTTLE_REASON:
            logging.debug(f"GPU {i} not throttled, reason={GPU_CLOCKS_THROTTLE_REASON[gpu_clock_throttle_out_line[0]]}")
         else:
            logging.debug(f"GPU {i} not throttled, reason={gpu_clock_throttle_out_line[0]}")
      else:
         logging.debug(f"GPU {i} not throttled, reason={gpu_clock_throttle_out_line[0]}")
if __name__ == "__main__":
   check_gpu_clock_throttling()