__author__ = 'abhisheksh'
import  numpy as np
from QueryAPI.TestParSub import TSub
import  multiprocessing as mp

class TMain:
    def __init__(self,n,mf):
        self.list=list(range(n))
        self.mf=mf
        print(len(self.list))
        print(self.list[0:5])



    def ProcessList(self):
        self.ts=TSub(self.mf)
        dl=[]
        '''
        for l in self.list:
            dl.append(ts.Double(l))
        '''
        pl=mp.Pool(7)
        dl=pl.map(self.ts.Double,self.list)
        pl.close()
        pl.join()

        print(len(dl))
        print(dl[0:5])
        print(dl[-5:-1])
        print(len(dl))




tm=TMain(10000,3)
tm.ProcessList()