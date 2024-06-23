class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        self.loger = log4j.LogManager.getLogger('com.example.firstapp')
