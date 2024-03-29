__author__ = 'abhisheksh'

import json
import meetup.api
import pandas as pd

from datadownloader.Utils.Logging import LoggingUtil
from datetime import  datetime
from datadownloader.APIClient.MeetupClients import MeetUpClients
from datadownloader.queryAPI.ExtractGroupInfo import GroupInfoExtractor as GrpExt
from datadownloader.queryAPI.ExtractCategoryInfo import CategoryInfoExtractor as CatExt
from datadownloader.queryAPI.ExtractEventInfo import EventInfoExtractor as EventExt
from datadownloader.queryAPI.ExtractEventDetails import EventInfoExtractor as EventDet
from datadownloader.queryAPI.ExtractMemberInfo import MemberInfoExtractor as MembExt

import os
import  multiprocessing as mp


class APIDataExtractionFacade:

    def __init__(self, configfolder, opfolder, confifgfile='MeetupKeys3.json', cattofind=None,
                 topic=None, locinfo=None, specificgroups=None):

        self.logfile = LoggingUtil(opfolder + 'Logs/', 'Started_API_Extraction_' + str(datetime.now()).replace(' ','_') + '.txt')
        self.opfolder = opfolder
        self.configfolder = configfolder
        self.specificgroupsfile = specificgroups
        self.cattofind = cattofind
        self.topictofind = topic
        logstr = "Category to find is ::" + str(self.cattofind) +  ", topic to find is ::  " + str(self.topictofind)
        self.logfile.Log(logstr)
        self.grp_ids_to_process_filtered = None
        self.allgroups_ids_urls_full = None

        self._group_info_extractor = None
        self._event_info_extractor = None
        self._category_info_extractor = None
        self._member_info_extractor = None
        self._event_det_extractor = None
        self.cattofind_id = None

        if locinfo is not None:
            locinfo_list = locinfo.split('|')
            if len(locinfo) < 2:
                logstr = "EX OCCURED : Location Requires City+Country in Case of Non US Locations"
                self.logfile.Log(logstr)
                raise Exception(logstr)
            self.city = locinfo_list[0]
            self.country = locinfo_list[1]
            self.state = locinfo_list[2]
            if self.country == 'US' and (len(locinfo) < 3 or self.state == ''):
                logstr = "Location Requires City+Country in Case of Non US Locations"
                self.logfile.Log(logstr)
                raise Exception(logstr)
        else:
            self.country = self.city = self.state = None

        self.group_pd = None
        if self.cattofind is None and self.topictofind is None:
            logstr = "EX OCCURED :  : Please Enter a Category or Topic to Extract"
            self.logfile.Log(logstr)
            raise Exception(logstr)

        self.meetup_clients = MeetUpClients(configfolder, confifgfile).clients
        self.keysatdisposal = len(self.meetup_clients)
        self.opfolder = opfolder
        #self.logfile=LoggingUtil(opfolder+'Logs/','Started_API_Extraction_'+str(datetime.now())+'.txt')

    def logdata(self, logstr, prnt = True):
        if prnt:
            print(logstr)
        self.logfile.Log(logstr)

    def load_groupinfo_fromfile(self):

        if self.group_pd is None:
            if self.specificgroupsfile is not None:
                self.group_pd = pd.read_csv(self.opfolder + 'Data/Groups/' + self.specificgroupsfile)
            else:
                self.group_pd = pd.read_csv(self.opfolder + 'Data/Groups/' + str(self.cattofind_id) + '_' + str(
                    self.topictofind) + '_Groups.csv')

        groups_to_process = self.group_pd
        # groups_to_process=self.group_pd[self.group_pd['city']=='Singapore']
        self.allgroups_ids_urls_full = list(zip(groups_to_process.id, groups_to_process.urlname))

    def start_info_extraction(self, groups=True, members=True, events=True, eventcountsextract=False):

        try:

            if groups:
                if self.specificgroupsfile is None:
                    if self.cattofind is not None:
                        self.extract_category_info()
                    else:
                        self.cattofind_id = None
                    # self.cattofind_id=9

                    self.extract_group_info()
                else:
                    self.load_groupinfo_fromfile()
                    self.extract_group_info()

                #groups_already_processed_for_members=[int(i.split('_')[0]) for i in os.listdir(self.opfolder +'Data/Members/')]
                #if(self.specificgroupsfile is not None):
                #        groups_to_be_reprocessed=self.allgroups_ids_urls_full
                #else:
                #    groups_to_be_reprocessed=[gf for gf in self.allgroups_ids_urls_full if(gf[0] not in groups_already_processed_for_members)]
                #print("Debug")
                #print(groups_to_be_reprocessed)
                #print(len(groups_to_be_reprocessed))
                #self.allgroups_ids_urls_full=groups_to_be_reprocessed

            if eventcountsextract:
                self.extract_eventinfo_counts()

            if events:
                self.load_groupinfo_fromfile()
                self.extract_event_info()

            if members:
                self.load_groupinfo_fromfile()
                self.extract_member_info()

        except Exception as e:
            print("Exception Occured")
            print(e)

    def extract_group_info(self):
        self._group_info_extractor = GrpExt(self.meetup_clients)
        logstr = " Finding number of groups for category " + str(self.cattofind) + " and topic to find is ::  " + \
                 str(self.topictofind) + "\n"
        self.logdata(logstr)
        totalgroupsincat = self._group_info_extractor.FindNumberOfGroups(self.cattofind_id, self.topictofind,
                                                                         self.country, self.city)
        logstr = " Total Number of Groups Present in category " + str(self.cattofind)+ "  and/or topics " + \
                 str(self.topictofind) + " :: " + str(totalgroupsincat)
        self.logdata(logstr)
        self.group_pd, methodlogtrace = self._group_info_extractor.ExtractGroupsOfCategoryRecursive(
            self.cattofind_id, self.topictofind, self.country, self.city ,self.opfolder, totalgroupsincat)
        self.logdata(methodlogtrace, prnt = False)

    def extract_category_info(self):
        try:
            self._category_info_extractor = CatExt(self.meetup_clients,self.opfolder, self.configfolder)
            self.cattofind_id = self._category_info_extractor.get_categoryid(self.cattofind)
            self.cattofind_id = self._category_info_extractor.get_categoryid(self.cattofind)

            logstr="Category Id of " + self.cattofind + " is : " + str(self.cattofind_id)
            self.logdata(logstr)
        except Exception as e:
            logstr = "Exception {} Occured while getting Category Id".format(str(e))
            self.logdata(logstr)
            raise ValueError(logstr)

    def extract_eventinfo_counts(self):
        totalgroups=len(self.allgroups_ids_urls_full)
        logstr = " No of groups in category Tech process for counts "+ str(totalgroups)
        self.logdata(logstr)
        self._event_info_extractor = EventExt(self.meetup_clients)
        exceptiongroups = []
        grp_ids_to_process = self.allgroups_ids_urls_full
        (EventCountsSuc, EventCountsFailed, methodlogtrace) = self._event_info_extractor.ExtractEventCountsRecursive(
            grp_ids_to_process, self.opfolder)
        self.logdata(methodlogtrace, prnt=False)

        print(" Successful events counts found for groups " + str(len(EventCountsSuc)) + " among total groups " +
              str(len(grp_ids_to_process)))
        print(" Events for which event count calculation failed are  " + str(len(EventCountsFailed)) +
              "among total groups" + str(len(grp_ids_to_process)))

    def extract_event_info(self):
        if self.grp_ids_to_process_filtered is None:
            self.grp_ids_to_process_filtered = self.allgroups_ids_urls_full

        if self._event_info_extractor is None or not('_event_info_extractor' in self.__dict__):
            self._event_info_extractor = EventExt(self.meetup_clients)

        (EventIdsSuc,trace,groups_went_into_exception)=self._event_info_extractor.ExtractGroupEventsRecursive(
            self.grp_ids_to_process_filtered, self.opfolder)
        self.logdata(trace, prnt=False)

        print(" Successful events found are " + str(len(EventIdsSuc)) + " among total groups"+
              str(len(self.grp_ids_to_process_filtered)))
        print(" Events for which event extraction failes are  " + str(len(groups_went_into_exception)) +
              " among total groups " + str(len(self.grp_ids_to_process_filtered)))

        if len(groups_went_into_exception) > 0:
            grps_event_failed = open(self.opfolder+'Grps_Event_Extraction_Failed.csv','a')
            for fg in groups_went_into_exception:
                grps_event_failed.write(str(fg)+'\n')
            grps_event_failed.close()

    def extract_member_info(self):
        self._member_info_extractor = MembExt(self.meetup_clients)
        grp_ids_to_process = self.allgroups_ids_urls_full
        (MemberIdsSuc, UnqMembers, strace, groups_went_into_exception) = self._member_info_extractor.\
            ExtractGroupMembersRecursive(grp_ids_to_process, self.opfolder)
        print(" Successful Unique Members found are " + str(len(UnqMembers)) + " among total groups " +
              str(len(grp_ids_to_process)))
        print(" Groups for which event extraction failes are  " + str(len(groups_went_into_exception)) +
              "among total groups"+str(len(grp_ids_to_process)))

        if len(groups_went_into_exception)>0:
            grps_event_failed = open(self.opfolder+'Grps_Member_Extraction_Failed.csv','a')
            for fg in groups_went_into_exception:
                grps_event_failed.write(str(fg)+'\n')
            grps_event_failed.close()

        for s in strace:
            self.logdata(s + '\n')

    def parallell_process(self, methodtoexecute, listtoprocess):
        poolprocessor = mp.Pool(7)
        resultafterparallelprocessing = poolprocessor.map(methodtoexecute, listtoprocess)
        poolprocessor.close()
        poolprocessor.join()
        return resultafterparallelprocessing

    def extract_eventdetails(self, event_details):
        events_to_process = []
        for idx, row in event_details.iterrows():
            try:
                events_to_process.append((row['id'],row['event_url'].split('/')[3]))
            except Exception as e:
                print("Failied for row {} with exception {}".format(row['id'], e))

        self._event_det_extractor = EventDet(self.meetup_clients)
        self._event_det_extractor.get_eventdetails(events_to_process)
        # for idx, row in event_details.iterrows():
        #     event_id = row['id']
        #     event_url = row['event_url'].split('/')[3]
        #     self._event_det_extractor.GetEventRSVPDetails(event_id, event_url)











