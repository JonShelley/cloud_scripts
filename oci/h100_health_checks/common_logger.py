import logging

runWithDummyValues = 0

# Common Logger
# Log Pattern:  {TIMEDATE}-\s([^\s\-]+)[\s\-]+(\S*)\,(\S*)\,(\S*)[\s\-]+(.*)
#        e.g.: 2024-06-14 13:25:47,831 - ERROR - GPU BW,2349XLG02D,dev1 - DtoH: 50.0 is below threshold: 52.0
# Usage : create instance of this logger
#         Use setters to set various values before calling log methods
class CommonLogger:

    def setLevel(self, level):
        self.logger.setLevel(level)

    def __init__(self, testname, hostSerial, device):
        self.testname = None
        self.hostSerial = None
        self.device = None
        self.logitem_seperator = ","
        self.set(testname, hostSerial, device)
        logging.basicConfig(level="INFO", format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('nhc')

    @classmethod
    def getLogger(cls, testname, hostSerial, device):
        return CommonLogger(testname, hostSerial, device)

    def reset(self):
        self.set(None, None, None)

    def set(self, testname, hostSerial, device):
        self.setTestName(testname)
        self.setHostSerial(hostSerial)
        self.setDevice(device)

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(self.getMsg(msg), *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(self.getMsg(msg), *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(self.getMsg(msg), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(self.getMsg(msg), *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(self.getMsg(msg), *args, **kwargs)

    def debug2(self, testName, msg, *args, **kwargs):
        self.logger.debug(self.getMsg2(testName, msg), *args, **kwargs)

    def info2(self, testName, msg, *args, **kwargs):
        self.logger.info(self.getMsg2(testName, msg), *args, **kwargs)

    def warning2(self, testName, msg, *args, **kwargs):
        self.logger.warning(self.getMsg2(testName, msg), *args, **kwargs)

    def error2(self, testName, msg, *args, **kwargs):
        self.logger.error(self.getMsg2(testName, msg), *args, **kwargs)

    def critical2(self, testName, msg, *args, **kwargs):
        self.logger.critical(self.getMsg2(testName, msg), *args, **kwargs)

    def setTestName(self, tn):
        if tn is None:
            self.testname = ""
        else:
            self.testname = tn

    def setDevice(self, dv):
        if dv is None:
            self.device = ""
        else:
            self.device = dv

    def setHostSerial(self, hs):
        if hs is None:
            self.hostSerial = ""
        else:
            self.hostSerial = hs

    def getMsg(self, msg):
        return self.getMsg2(self.testname, msg)

    def getMsg2(self, testName, msg):
        return str(testName) + self.logitem_seperator + str(self.hostSerial) \
               + self.logitem_seperator + str(self.device) + " - " + str(msg)




#commons logger
#logger = CommonLogger("Main", None, None)

if __name__ == '__main__':
    logger = CommonLogger.getLogger(None, None, None)
    logger.setLevel('DEBUG')
    logger.critical("message comes here")
    logger.error("message comes here")
    logger.warning("message comes here")
    logger.info("message comes here")
    logger.setTestName("Test1")
    logger.debug("message comes here")
    logger.setHostSerial("serHost1")
    logger.critical("message comes here")
    logger.setDevice("dev1")
    logger.critical2("test3", "message comes here")
    logger.set("Test4", "hostSr1", "dev2")
    logger.critical2("test3", "message comes here")
