import subprocess

from common_logger import CommonLogger, runWithDummyValues


class GPURemapTest:

    def __init__(self):
        self.testname = "GPU REMAP"
        self.logger = CommonLogger.getLogger(self.testname, None, None)
        
    def check_row_remap_errors(self):
        remap_issues = []
        if bool(runWithDummyValues):
            return self.getDummyResults()

        try:
            # Run the nvidia-smi -q command
            result = subprocess.run(['nvidia-smi', '--query-remapped-rows=remapped_rows.pending,remapped_rows.failure,remapped_rows.uncorrectable', '--format=csv,noheader'], stdout=subprocess.PIPE)
    
            if result.returncode != 0:
                self.logger.debug(f"Check row remap command exited with error code: {result.returncode}")
    
        except FileNotFoundError:
            self.logger.warning("Skipping Row Remap Test: nvidia-smi command not found")
            return []
    
        # Decode the output from bytes to string
        output = result.stdout.decode('utf-8')
        self.logger.debug("Output: {}".format(output))
        for i, line in enumerate(output.split('\n')):
            if line == "":
                continue
            tmp_data = line.split(",")
            tmp_data = [x.strip() for x in tmp_data]
            if tmp_data[0] != "0":
                self.logger.debug(f"GPU: {i} - Row Remap Pending: {tmp_data[0]}")
                remap_issues.append(f"GPU: {i} Row Remap Pending: {tmp_data[0]}")
            if tmp_data[1] != "0":
                self.logger.debug(f"GPU: {i} - Row Remap Failure: {tmp_data[1]}")
                #remap_issues.append(f"GPU: {i} Row Remap Failure: {tmp_data[1]}")
            if tmp_data[2] != "0":
                self.logger.debug(f"GPU: {i} - Row Remap Uncorrectable: {tmp_data[2]}")
                if int(tmp_data[2]) > 512:
                    remap_issues.append(f"GPU: {i} - Row Remap Uncorrectable >512: {tmp_data[2]}")
                else:
                    remap_issues.append(f"GPU: {i} - Row Remap Uncorrectable <512: {tmp_data[2]}")# Check if there are ecc_issues
    
        if len(remap_issues) == 0:
            self.logger.info("GPU Remap Test: Passed")
        else:
            self.logger.warning("GPU Remap Test: Failed")
    
        return remap_issues


    def logResults(self, remap_results):
        remap_error=False
        if len(remap_results) > 0:
            for issue in remap_results:
                if "<512" in issue:
                    self.logger.warning(f"{issue}")
                else:
                    self.logger.error(f"{issue}")
                    remap_error=True
        return remap_error

    @classmethod
    def getDummyResults(cls):
        remap_results = []
        remap_results.append(f"GPU: 1 - Row Remap Uncorrectable <512: 400")
        remap_results.append(f"GPU: 2 - Row Remap Uncorrectable >512: 600")
        return remap_results

if __name__ == '__main__':
    #runWithDummyValues = 1
    gpuremap = GPURemapTest()
    remap_results = gpuremap.check_row_remap_errors()
    gpuremap.logResults(remap_results)

