__author__ = 'abhisheksh'
import pandas as pd
import  multiprocessing as mp
import  json
from functools import  partial

class GroupInfoExtractor:
    def __init__(self,meetupclients):
        self.meetupclients=meetupclients
        self.num_of_clients = len(meetupclients)
        #self.FindNumberOfGroups()



    def FindNumberOfGroups(self,categoryid=None,topicIN=None,countryIN=None,cityIN=None):
        #techgroups=[]
        techgroupsall=[]
        mtupcl=self.meetupclients[0][1]
        if(categoryid is not  None):
            catid=int(categoryid)
        else:
            catid=categoryid

        totalgroupsincat=mtupcl.GetGroups(category_id=catid,city=cityIN,country=countryIN,topic=topicIN,page=200,offset=0).meta['total_count']
        return totalgroupsincat



    #def FindGroups(categoryid,topicIN,offsetid,reprocess=None):
    #def FindGroups(self,categoryid,topicIN,offsetid,reprocess=None):
    def FindGroups(self,grpinfo):
        categoryid=grpinfo[0]
        topicIN=grpinfo[1]
        offsetid=grpinfo[2]
        reprocess=grpinfo[3]
        countryIN=grpinfo[4]
        cityIN=grpinfo[5]
        opfolder=grpinfo[6]
        cl=self.meetupclients[int(offsetid%self.num_of_clients)]
        logstr=" Invoked Client " + str(cl[0])
        #currmethodtrace=logstr+"\n"
        print(logstr)

        if(categoryid is not  None):
            catid=int(categoryid)
        else:
            catid=categoryid

        if(reprocess is not None):
            if(reprocess):
                logstr= " Reprocessing For Offset"  + str(offsetid)
                currmethodtrace=logstr+"\n"
            else:
                reprocess=False

        mtupcl=cl[1]
        currmethodtrace=""

        tpc=topicIN
        #tpc='deep-learning'
        #c=None
        #c='Singapore'
        try:
            groupinfo=mtupcl.GetGroups(category_id=categoryid,city=cityIN,country=countryIN,topic=tpc,page=200,offset=offsetid)
            numofgroups=len(groupinfo.results)
            if(reprocess):
                logstr="Total meetup groups found while reprocessing for offset  " + str(offsetid) + " are "+ str(numofgroups)
                currmethodtrace=logstr+"\n"
                print(logstr)
            else:
                logstr="Total meetup groups found for offset  " + str(offsetid) + " are "+ str(numofgroups)
            currmethodtrace=logstr+"\n"
            print(logstr)

            if(numofgroups>0):
                #techgroups+=groupinfo.results
                if(reprocess):
                    pd.read_json(json.dumps(groupinfo.results)).to_csv(opfolder+"Data/Groups/reprocess_"+str(offsetid)+"_"+str(categoryid)+'_Groups.csv',index=False)
                else:
                     pd.read_json(json.dumps(groupinfo.results)).to_csv(opfolder+"Data/Groups/"+str(offsetid)+"_"+str(categoryid)+'_Groups.csv',index=False)
            else:
                logstr="No groups found for offset " + str(offsetid)
                currmethodtrace=logstr+"\n"
                print(logstr)
                return (None,currmethodtrace,offsetid,grpinfo)
            #print(type(groupinfo.results))
            print()


            return  (groupinfo.results,currmethodtrace,offsetid,grpinfo)
        except:
                if(reprocess):
                    logstr="Exception Occured for Offset while reprocessing : " + str(offsetid)
                    currmethodtrace=logstr+"\n"
                    print(logstr)

                else:
                    logstr="Exception Occured for Offset : " + str(offsetid)
                    currmethodtrace=logstr+"\n"
                    print(logstr)

                #offsetswhichwhentintoexception.append(offsetid)
                print(logstr)
                return (None,currmethodtrace,offsetid,grpinfo)



    def ExtractGroupInfoParalallel(self,listtoprocesss):
        #for g in list(zip(catlist,topiclist,offsetsrange,rplist,opfolderlist)):
        #    self.FindGroups(g)

        #techgroups=pooltofindgroups.map(FindGroups,offsetsrange)
        #techgroups=pooltofindgroups.map(partial(self.FindGroups,reprocess=False),offsetsrange)
        #techgroups=pooltofindgroups.map(partial(self.FindGroups,categoryid,topic,reprocess=False),offsetsrange)
        pooltofindgroups=mp.Pool()
        techgroups=pooltofindgroups.map(self.FindGroups,listtoprocesss)
        pooltofindgroups.close()
        pooltofindgroups.join()
        return techgroups


    def ExtractGroupsOfCategoryRecursive(self,categoryid,topic,country,city,opfolder, totalgroupsincat = None):
        totalgroupsincat=self.FindNumberOfGroups(categoryid,topic,country,city)
        offsetsmax=int(totalgroupsincat/200)+1
        offsetsrange=list(range(offsetsmax+1))
        #logstr="Total Number of Offsets::" + str(offsetsmax)
        #self.logfile.Log(logstr)
        #print(logstr)

        catlist=[categoryid]*(offsetsmax+1)
        topiclist=[topic]*(offsetsmax+1)
        rplist=[False]*(offsetsmax+1)
        countrylist=[country]*(offsetsmax+1)
        citylist=[city]*(offsetsmax+1)



        opfolderlist=[opfolder]*(offsetsmax+1)
        offsetlist=list(zip(catlist,topiclist,offsetsrange,rplist,countrylist,citylist,opfolderlist))

        techgroups=self.ExtractGroupInfoParalallel(offsetlist)




        reprcount=0
        offsetswhichwhentintoexception=[g[3] for g in techgroups  if("Exception Occured " in g[1])]
        techgroupsall=[g for g in techgroups  if("Exception Occured " not in g[1])]

        while(reprcount<5 and len(offsetswhichwhentintoexception)>0):
            reppffsets=self.ExtractGroupInfoParalallel(offsetswhichwhentintoexception)
            offsetswhichwhentintoexception=[g[3] for g in reppffsets  if("Exception Occured " in g[1])]
            succrepoffsets=[g for g in reppffsets  if("Exception Occured " not in g[1])]
            techgroupsall=techgroupsall+succrepoffsets
            reprcount+=1




        '''
        for info in techgroups:
            logfile.Log(info[1],newline=True)
            if("Exception Occured " in info[1]):
                offsetswhichwhentintoexception.append(int(info[1].split(':')[1]))


        for i in offsetswhichwhentintoexception:
            logstr=" Exceptions Occured for Offsets :: " + str(offsetswhichwhentintoexception)
            logfile.Log(logstr)
            print(logstr)

        for eo in offsetswhichwhentintoexception:
            #del techgroups[eo]
            techgroups.append(FindGroups(eo,reprocess=True))
        '''

        print(len(techgroupsall))
        techgroupsall_new=[gi for g in techgroupsall if(g[0] is not None) for gi in g[0] ]
        group_pd=pd.read_json(json.dumps(techgroupsall_new))
        group_pd.to_csv(opfolder+"Data/Groups/"+str(categoryid)+'_'+str(topic)+'_Groups.csv',index=False)
        return group_pd



