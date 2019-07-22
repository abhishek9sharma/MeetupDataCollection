__author__ = 'abhisheksh'
import datetime

class ExceptionUtil:
        def __init__(self,path,name):
            self.timeinitialized=datetime.datetime.now()
            self.exceptionfilepath=path+name
            self.IntializeFile()

        def IntializeFile(self):
            self.excpfile=open(self.exceptionfilepath,'a')
            self.LogException(" File Initialized at  " + str(self.timeinitialized))




        def LogException(self,excpstring):
            self.excpfile.write(excpstring+'\n')

