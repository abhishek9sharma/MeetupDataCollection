__author__ = 'abhisheksh'

import json
import meetup.api
import  pandas as pd

from  datadownloader.Utils.Logging import LoggingUtil
from datetime import  datetime
from datadownloader.APIClient.MeetupClients import MeetUpClients
from datadownloader.QueryAPI.ExtractGroupInfo import GroupInfoExtractor as GrpExt
from datadownloader.QueryAPI.ExtractCategoryInfo import CategoryInfoExtractor as CatExt
from datadownloader.QueryAPI.ExtractEventInfo import EventInfoExtractor as EventExt
from datadownloader.QueryAPI.ExtractMemberInfo import MemberInfoExtractor as MembExt

import os
import  multiprocessing as mp


class APIDataExtractionFacade:

    def __init__(self,mainfolder,opfolder,config=None,cattofind=None,topic=None,locinfo=None,specificgroups=None):
        self.specificgroupsfile=specificgroups
        self.cattofind=cattofind
        self.topictofind=topic
        if(locinfo is not None):
            locinfo_list=locinfo.split('|')
            if(len(locinfo)<2):
                raise Exception(" Location Requires City+Country in Case of Non US Locations")
            self.city= locinfo_list[0]
            self.country= locinfo_list[1]
            self.state=locinfo_list[2]
            if(self.country=='US' and (len(locinfo)<3 or self.state=='')):
                raise Exception(" Location Requires City+Country+State  in Case of Non US Locations")
        else:
            self.country=self.city=self.state=None

        self.group_pd=None
        if(self.cattofind is None and self.topictofind is None):
            raise Exception(" Please Enter a Category or Topic to Extract")


        self.meetup_clients=MeetUpClients(configfolder=mainfolder).clients
        self.keysatdisposal=len(self.meetup_clients)
        self.opfolder=opfolder
        self.logfile=LoggingUtil(opfolder+'Logs/','Started_API_Extraction_'+str(datetime.now())+'.txt')


    def Log(self,logstr):
        print(logstr)
        self.logfile.Log(logstr)


    def StartInfoExtraction(self):
        try:


            if(self.cattofind is not  None):
                self.ExtractCategoryInfo()
            else:
                self.cattofind_id=None
            # self.cattofind_id=9

            self.ExtractGroupInfo()


            if(self.group_pd is None):
                if(self.specificgroupsfile is not None):
                    self.group_pd=pd.read_csv(opfolder+'Data/Groups/'+self.specificgroupsfile)
                else:
                    self.group_pd=pd.read_csv(opfolder+'Data/Groups/'+str(self.cattofind_id)+'_'+str(self.topictofind)+'_Groups.csv')
            groups_to_process=self.group_pd
            #groups_to_process=self.group_pd[self.group_pd['city']=='Singapore']
            self.allgroups_ids_urls_full=list(zip(groups_to_process.id,groups_to_process.urlname))
            groups_already_processed_for_members=[int(i.split('_')[0]) for i in os.listdir(self.opfolder +'Data/Members/')]

            if(self.specificgroupsfile is not None):
                    groups_to_be_reprocessed=self.allgroups_ids_urls_full
            else:
                groups_to_be_reprocessed=[gf for gf in self.allgroups_ids_urls_full if(gf[0] not in groups_already_processed_for_members)]
            print("Debug")
            print(groups_to_be_reprocessed)
            print(len(groups_to_be_reprocessed))
            self.allgroups_ids_urls_full=groups_to_be_reprocessed



            self.ExtractEventInfo()
            self.ExtractMemberInfo()

        except Exception as e:
            print("Exception Occured")
            print(e)


    def ExtractGroupInfo(self):
        self._group_info_extractor=GrpExt(self.meetup_clients)
        totalgroupsincat=self._group_info_extractor.FindNumberOfGroups(self.cattofind_id,self.topictofind,self.country,self.city)
        logstr=" Total Number of Groups Present in category " + str(self.cattofind)+ "  and/or topics " +str(self.topictofind) + " :: "+ str(totalgroupsincat)
        self.Log(logstr)
        self.group_pd=self._group_info_extractor.ExtractGroupsOfCategoryRecursive(self.cattofind_id,self.topictofind,self.country,self.city,self.opfolder, totalgroupsincat)


    def ExtractCategoryInfo(self):
        try:
            self._category_info_extractor=CatExt(self.meetup_clients,self.opfolder)
            self.cattofind_id=self._category_info_extractor.GetCategoryId(self.cattofind)
            logstr="Category Id of "+ self.cattofind + " is : " + str(self.cattofind_id)
            self.Log(logstr)
        except:
            logstr="Exception Occured while getting Category Id"
            self.Log(logstr)
            raise ValueError(logstr)



    def ExtractEventInfo(self):
        totalgroups=len(self.allgroups_ids_urls_full)
        logstr= " No of groups in category Tech process for counts "  + str(totalgroups)
        self.Log(logstr)

        self._event_info_extractor=EventExt(self.meetup_clients)
        exceptiongroups=[]
        grp_ids_to_process=self.allgroups_ids_urls_full
        (EventCountsSuc,EventCountsFailed)=self._event_info_extractor.ExtractEventCountsRecursive(grp_ids_to_process,self.opfolder)
        print(" Successful events counts found for groups " + str(len(EventCountsSuc)) + " among total groups "+  str(len(grp_ids_to_process)))
        print(" Events for which event count calculation failed are  " + str(len(EventCountsFailed)) + " among total groups "+  str(len(grp_ids_to_process)))


        (EventIdsSuc,trace,groups_went_into_exception)=self._event_info_extractor.ExtractGroupEventsRecursive(grp_ids_to_process,self.opfolder)
        print(" Successful events found are " + str(len(EventIdsSuc)) + " among total groups "+  str(len(grp_ids_to_process)))
        print(" Events for which event extraction failes are  " + str(len(groups_went_into_exception)) + " among total groups "+  str(len(grp_ids_to_process)))

        #for t in trace:
        #    self.Log(trace)

        if(len(groups_went_into_exception)>0):
            grps_event_failed=open(self.opfolder+'Grps_Event_Extraction_Failed.csv','a')
            for fg in groups_went_into_exception:
                grps_event_failed.write(str(fg)+'\n')
            grps_event_failed.close()


    def ExtractMemberInfo(self):
        self._member_info_extractor=MembExt(self.meetup_clients)
        grp_ids_to_process=self.allgroups_ids_urls_full
        (MemberIdsSuc,UnqMembers,strace,groups_went_into_exception)=self._member_info_extractor.ExtractGroupMembersRecursive(grp_ids_to_process,self.opfolder)
        print(" Successful Unique Members found are " + str(len(UnqMembers)) + " among total groups "+  str(len(grp_ids_to_process)))
        print(" Groups for which event extraction failes are  " + str(len(groups_went_into_exception)) + " among total groups "+  str(len(grp_ids_to_process)))

        #print(strace)
        if(len(groups_went_into_exception)>0):
            grps_event_failed=open(self.opfolder+'Grps_Member_Extraction_Failed.csv','a')
            for fg in groups_went_into_exception:
                grps_event_failed.write(str(fg)+'\n')
            grps_event_failed.close()

        for s in strace:
            self.Log(s+'\n')


    def ParallellProcess(self,methodtoexecute,listtoprocess):
        poolprocessor=mp.Pool(7)
        ResultAfterParallelProcessing=poolprocessor.map(methodtoexecute,listtoprocess)
        poolprocessor.close()
        poolprocessor.join()
        return  ResultAfterParallelProcessing

        #self._event_info_extractor.ExtractEventCountsRecursive(self.allgroups_ids_urls_full[0:7],self.opfolder)






#mainfolder='/home/abhisheksh/PROJECTS/MeetupAPI/'
#mainfolder='/media/oldmonk/G/CW_Research/TERM11/MSR2018PROJECT/MeetupAPI/'
#opfolder=mainfolder+'Tech_ALL/'
#opfolder=mainfolder+'deep-learning/'
#mainfolder='../'
#opfolder=mainfolder+'/techall/'
#opfolder=mainfolder+'/../fitness/'
#opfolder=mainfolder+'/../sanskrit/'

#print(os.listdir(mainfolder))

#cat='Tech'
# cat='Fitness'
#cat = None
#topictofind='sanskrit'
#topictofind = None
#cat=None
#topictofind='deep-learning'
#topictofind='python'
#city='Singapore'
#country='SG'
#city_country_state='Singapore|SG|'
#city_country_state=None
#apide=APIDataExtractionFacade(mainfolder,opfolder,cattofind=cat,topic=topictofind,locinfo=city_country_state,specificgroups='final_esem_swdevonly_missing_groups_members.csv')

# for i in os.listdir(opfolder+'/Data/Groups'):
#     spgroups = i
#     apide=APIDataExtractionFacade(mainfolder,opfolder,cattofind=cat,topic=topictofind,locinfo=city_country_state,specificgroups=spgroups)
#     apide.StartInfoExtraction()



mainfolder= os.path.dirname(os.path.realpath(__file__))
cat = None
topictofind = 'sanskrit'
city_country_state = None

if cat:
    foldername = cat
elif topictofind:
    foldername = topictofind
else:
    raise ValueError(" Please set a catgeory or topic whose data has to be extracted")

opfolder = mainfolder+'/../../'+ foldername +'/'


apide=APIDataExtractionFacade(mainfolder,opfolder,cattofind=cat,topic=topictofind,locinfo=city_country_state,specificgroups=None)
apide.StartInfoExtraction()






