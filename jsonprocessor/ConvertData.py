__author__ = 'abhisheksh'
from jsonprocessor.GroupProcessor import  ProcessGroups
from jsonprocessor.MembersProcessor import ProcessGroupsMembers
from jsonprocessor.EventsProcessor import ProcessGroupsEvents
import gc

class ConvertData:
    def __init__(self,mainfolder,groupfilename,memberfilesfolder,eventfilesfolder,customfolder,reprocessevent=False,reprocessmember=False,\
                 processed_groups=[]):
        self._group_data_processor=ProcessGroups(mainfolder+groupfilename,mainfolder+customfolder+'CSVFormat/Groups/')
        self._member_data_processor=ProcessGroupsMembers(mainfolder+groupfilename,mainfolder+memberfilesfolder,\
                                                         mainfolder+customfolder+'CSVFormat/Members/',\
                                                         reprocessmember,\
                                                         processed_groups)
        self._event_data_processor = ProcessGroupsEvents(mainfolder+groupfilename,mainfolder + eventfilesfolder,mainfolder +customfolder+ 'CSVFormat/Events/',reprocessevent)

    def StartConversion(self,membersplitnumber=0):

        #Convert and Write Group Data

        self._group_data_processor.ConvertAllGroupInfotoCSV()
        self._group_data_processor.WriteConvertedCSV()

        #Convert and Write Event Data
        self._event_data_processor.ProcessAllGroupEventFiles()
        self._event_data_processor.WriteConvertedCSV()
        self._event_data_processor.WriteFailedGroups()

        #Convert and Write Member Data

        if(membersplitnumber!=0):
            splitsize = int(len(self._member_data_processor.groups_members_files) / membersplitnumber)
            self._member_data_processor.groups_members_files_sublists = [self._member_data_processor.groups_members_files[i:i + splitsize] for i in
                                                  range(0, len(self._member_data_processor.groups_members_files), splitsize)]

            for sublistnumber in range(membersplitnumber):
                print(" Processing Member Sublist " + str(sublistnumber))
                self._member_data_processor.ProcessAllGroups_Members(sublistnumber)
                #self._member_data_processor.ProcessAllGroups_Members_LinearAndPArallel(4)
                self._member_data_processor.WriteConvertedCSV(sublistnumber)
                self._member_data_processor.WriteFaileGroups(sublistnumber)


                self._member_data_processor.group_members_df_all_combined=None
                self._member_data_processor.group_members_topics_df_combined=None
                self._member_data_processor.group_members_joined_df_combined=None
                self._member_data_processor.members_processed=[]
                gc.collect()

        else:
            self._member_data_processor.ProcessAllGroups_Members()
            #self._member_data_processor.ProcessAllGroups_Members_LinearAndPArallel(4)
            self._member_data_processor.WriteFailedGroups()
        gc.collect()




# #folderpath='/media/oldmonk/SAMSUNG/DATA_ABHISHEK/MEETUPPROJECT/techall/Data/'
# #groupfile='Groups/.csv'
#
# cat ='Fitness'
# folderpath='../../'+ cat  +'/Data/'
# #groupfile='Groups/final_esem_swdevonly_all.csv'
# #groupfile='Groups/final_esem_swdevonly_missing_groups_events.csv'
# #groupfile='Groups/final_esem_swdevonly_missing_groups_members.csv'
# groupfile='Groups/9_None_Groups.csv'
#
#
# #folderpath='../deep-learning/Data/'
# #groupfile='Groups/34_deep-learning_Groups.csv'
#
#
#
#
# groupfile='Groups/Tech_Groups_Filtered_city.csv'
# #domain=''
# #groupfile='Groups/34_python_Groups.csv'
# #groupfile='Groups/34_None_Groups.csv'
#
#
# memberfilesfolder='Members/'
# eventfilesfolder='Events/'
#
#
#
#
#
#

# folderpath = '../Tech/Data'
# memberfilesfolder='Members/'
# eventfilesfolder='Events/'
# groupfile = 'groups_converted.csv'
# cd=ConvertData(folderpath,groupfile,memberfilesfolder,eventfilesfolder,'',False,True)
# cd.StartConversion()