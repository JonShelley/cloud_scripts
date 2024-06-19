import re
import subprocess

from common_logger import CommonLogger, runWithDummyValues


class ECCTest:

    def __init__(self):
        self.testname = "ECC"
        self.logger = CommonLogger.getLogger(self.testname, None, None)

    def check_ecc_errors(self):
        ecc_issues = []
        if bool(runWithDummyValues):
            return self.getDummyResults()
        try:
            # Run the nvidia-smi -q command
            result = subprocess.run(['nvidia-smi', '-q'], stdout=subprocess.PIPE)
        except FileNotFoundError:
            self.logger.warning("Skipping SRAM/DRAM ECC Test: nvidia-smi command not found")
            return []

        # Decode the output from bytes to string
        output = result.stdout.decode('utf-8')

        # Find the lines containing "SRAM Correctable" and "DRAM Correctable"
        sram_matches = re.findall(r'SRAM Uncorrectable\s+:\s+(\d+)', output)
        if len(sram_matches)==0:
            sram_matches = re.findall(r'SRAM Uncorrectable Parity\s+:\s+(\d+)', output)
        dram_matches = re.findall(r'DRAM Uncorrectable\s+:\s+(\d+)', output)
        gpu_matches = re.findall(r'\nGPU\s+(.*)\n', output)
        vol_sram_line = sram_matches[0::2]
        vol_dram_line = dram_matches[0::2]
        agg_sram_line = sram_matches[1::2]
        agg_dram_line = dram_matches[1::2]

        for i, gpu in enumerate(gpu_matches):
            self.logger.debug(f"GPU: {gpu}")
            if vol_sram_line[i] != "0":
                self.logger.debug(f"Volatile SRAM Uncorrectable: {vol_sram_line[i]}")
                ecc_issues.append(f"{gpu_matches[i]} - Volatile SRAM Uncorrectable: {vol_sram_line[i]}")
            if vol_dram_line[i] != "0":
                self.logger.debug(f"Volatile DRAM Uncorrectable: {vol_dram_line[i]}")
                ecc_issues.append(f"{gpu_matches[i]} - Volatile DRAM Uncorrectable: {vol_dram_line[i]}")
            if agg_sram_line[i] != "0":
                self.logger.debug(f"Aggregate SRAM Uncorrectable: {agg_sram_line[i]}")
                ecc_issues.append(f"{gpu_matches[i]} - Aggregate SRAM Uncorrectable: {agg_sram_line[i]}")
            if agg_dram_line[i] != "0":
                self.logger.debug(f"Aggregate DRAM Uncorrectable: {agg_dram_line[i]}")
                ecc_issues.append(f"{gpu_matches[i]} - Aggregate DRAM Uncorrectable: {agg_dram_line[i]}")


        # Check if there are ecc_issues
        if len(ecc_issues) == 0:
            self.logger.info("GPU ECC Test: Passed")
        else:
            self.logger.warning("GPU ECC Test: Failed")

        return ecc_issues

    def logResults(self, ecc_issues, host_serial):
        ecc_error=False
        if len(ecc_issues) > 0:
            self.logger.setHostSerial(host_serial)
            for issue in ecc_issues:
                if "Skipped" in issue:
                    self.logger.warning(f"{issue}")
                else:
                    if "Aggregate" in issue:
                        self.logger.warning(f"{issue}")
                    else:
                        self.logger.error(f"{issue}")
                        ecc_error=True
        return ecc_error

    @classmethod
    def getDummyResults(cls):
        ecc_issues = []
        ecc_issues.append(f"GPU1 - Volatile SRAM Uncorrectable: VOL_LINE1")
        ecc_issues.append(f"GPU1 - Aggregate DRAM Uncorrectable: AGG_LINE1")
        return ecc_issues

if __name__ == '__main__':
    #runWithDummyValues = 1
    ecc = ECCTest()
    ecc_issues = ecc.check_ecc_errors()
    ecc.logResults(ecc_issues, "host_serial1")

