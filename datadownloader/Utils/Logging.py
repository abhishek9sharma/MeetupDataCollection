__author__ = 'abhisheksh'

__author__ = 'abhisheksh'
import datetime

class LoggingUtil:
        def __init__(self,path,name):
            self.timeinitialized=datetime.datetime.now()
            self.logfilepath=path+name
            self.IntializeFile()

        def IntializeFile(self):
            self.Logfile=open(self.logfilepath,'a')
            self.Log(" File Initialized at  " + str(self.timeinitialized))




        def Log(self,logstring,newline=False):
            if(newline):
                self.Logfile.write(logstring)
            else:
                self.Logfile.write(logstring+'\n')

