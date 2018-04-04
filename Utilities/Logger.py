DEBUG = False


class Logger:

    def __init__(self, prefix):
        self.prefix = prefix

    def debug(self, msg):
        if DEBUG:
            print("[DEBUG] %s %s" % (self.prefix, msg))

    def info(self, msg):
        print("[INFO] %s %s" % (self.prefix, msg))

    def warn(self, msg):
        print("[WARN] %s %s" % (self.prefix, msg))

    def error(self, msg):
        print("[ERROR] %s %s" % (self.prefix, msg))
