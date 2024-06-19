import subprocess

from common_logger import CommonLogger, runWithDummyValues


class RTTCCTest:

    def __init__(self, inRoot):
        self.testname = "RTTCC"
        self.logger = CommonLogger.getLogger(self.testname, None, None)
        self.inRoot = inRoot

    def check_rttcc_status(self):
        if bool(runWithDummyValues):
            return RTTCCTest.getDummyResults()
        link_status = {}
        devices = ["mlx5_0", "mlx5_1", "mlx5_3", "mlx5_4", "mlx5_5", "mlx5_6", "mlx5_7", "mlx5_8", "mlx5_9", "mlx5_10", "mlx5_12", "mlx5_13", "mlx5_14", "mlx5_15", "mlx5_16", "mlx5_17"]
        status = "disabled"
        status_dict = {"devices": {}}
        for device in devices:
            if not self.inRoot:
                command = ['sudo', 'mlxreg', '-d', device, '-y', '--get', '--reg_name=PPCC', '--indexes=local_port=1,pnat=0,lp_msb=0,algo_slot=0,algo_param_index=0']
            else:
                command = ['mlxreg', '-d', device, '-y', '--set', 'cmd_type=3', '--reg_name=PPCC', '--indexes=local_port=1,pnat=0,lp_msb=0,algo_slot=0,algo_param_index=0']
            result = subprocess.run(command, stdout=subprocess.PIPE)
            output = result.stdout.decode('utf-8')
            filtered_output = [line for line in output.split('\n') if line.startswith('value')]
            for line in filtered_output:
                self.logger.debug(line)
                if "0x00000001" in line:
                    status_dict["devices"][device] = "enabled"

        for device in status_dict["devices"]:
            if status_dict["devices"][device] == "enabled":
                self.logger.warning(f"RTTCC enabled on {device}")
                status = "enabled"
                if device not in link_status:
                    link_status[device] = []
                link_status[device].append(f"RTTCC enabled")
            else:
                self.logger.info(f"RTTCC status for {device}: disabled")

        self.logger.setDevice(None)
        if status == "disabled":
            self.logger.info(f"RTTCC disabled check: Passed")
        else:
            self.logger.error(f"RTTCC disabled check: Failed")

        return link_status

    def logResults(self, rttcc_issues):
        if len(rttcc_issues) > 0:
            for dev in rttcc_issues:
                self.logger.setDevice(dev)
                self.logger.error(f"{rttcc_issues[dev]}")

    @classmethod
    def getDummyResults(cls):
        devices = ["mlx5_0", "mlx5_1", "mlx5_3"]
        ret = {}
        for dev in devices:
            ret[dev] = []
            ret[dev].append("RTTCC enabled")
        return ret

if __name__ == '__main__':
    #runWithDummyValues = 1
    rt = RTTCCTest(True)
    rttcc_issues = rt.check_rttcc_status()
    rt.logResults(rttcc_issues)

