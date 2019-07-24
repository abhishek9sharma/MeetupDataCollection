__author__ = 'abhisheksh'
from jsonprocessor.TransformationUtil import TransformHelper as thlp
import pandas as pd
import  os
from jsonprocessor.EventProcessor import ProcessSingleGroupEvents as EP
from functools import reduce
import  multiprocessing as mp

class ProcessGroupsEvents:

    def __init__(self,groupfilecsv,eventsfolder,opfolder,reprocessMode=False):
        self.eventsfolder=eventsfolder
        self.opfolder=opfolder
        self.group_pdframe = pd.read_csv(groupfilecsv)
        self.groups_events_files=os.listdir(self.eventsfolder)
        self.validgroupids=[str(id) for id in self.group_pdframe['id'].tolist()]
        self.groups_events_files=[f for f in self.groups_events_files if ((f.split('_')[0]  in self.validgroupids)
                                                                          or (f.split('_')[1]  in self.validgroupids)) ]


        self.group_events_df_all_combined=None
        #self.group_events_joined_df_combined=None
        self.groups_events_processed=[]
        self.group_events_failed=[]
        self.reprocessMode=reprocessMode



    def ProcessGroupEventFile(self,f):
        e_pro = EP(self.eventsfolder + f, self.opfolder + 'Events_Groups_Individual/')
        try:
            if(e_pro.emptyfile):
                return  e_pro
            else:
                e_pro.ProcessGroupEventInfo()
                e_pro.WriteConvertedCSV()
                return  e_pro
        except Exception as e:
            print(" Below Exception Occured whiel Processing Events for Group " + str(e_pro.group_id))
            print(e)
            self.exceptionoccured = True
            return e_pro

    def ProcessAllGroupEventFiles(self):
        groups_events_processed=[]

        '''
        for f in self.groups_events_files[0:2]:
            e_pro=self.ProcessGroupEventFile(f)
            self.groups_events_processed.append(e_pro)
            #print("Debug")


        '''
        cores=mp.cpu_count()-1
        pooltofind_events=mp.Pool(cores)
        self.groups_events_processed=pooltofind_events.map(self.ProcessGroupEventFile,self.groups_events_files)
        pooltofind_events.close()
        pooltofind_events.join()



        self.group_events_df_all_combined=pd.concat([e.group_events_df_all for e in self.groups_events_processed if (e.emptyfile==False and e.exceptionoccured == False)])

        self.group_events_failed = [e for e in self.groups_events_processed if
                                     (e.emptyfile == True or e.exceptionoccured == True)]



        self.eventfileswritten=os.listdir(self.opfolder + 'Events_Groups_Individual/')
        print(len(self.eventfileswritten))

        #self.group_events_failed_ids=[g.split(_)[0] for g in self.eventfileswritten]


        #self.group_members_joined_df_combined=pd.concat([m.group_members_joined_df for m in self.members_processed])
        #self.group_members_topics_df_combined=pd.concat([m.group_members_topics_df for m in self.members_processed])
        #self.group_members_topics_df_combined=self.group_members_topics_df_combined.drop_duplicates()

        print("Events Shape before duplicate removal " + str(self.group_events_df_all_combined.shape))
        self.group_events_df_all_combined=self.group_events_df_all_combined.drop_duplicates()
        print("Events Shape after duplicate removal " + str(self.group_events_df_all_combined.shape))



    def WriteConvertedCSV(self):
        combfolder='/Events_Groups_Combined/'
        if(self.reprocessMode):
            existingdf=pd.read_csv(self.opfolder+combfolder+'events_converted_combined.csv')
            newdf=pd.concat([existingdf,self.group_events_df_all_combined])
            newdf=newdf.drop_duplicates()

            newdf.to_csv(self.opfolder+combfolder+'events_converted_combined_reprooceseed.csv',index=False)
        else:
            self.group_events_df_all_combined.to_csv(self.opfolder+combfolder+'events_converted_combined.csv',index=False)



    def WriteFailedGroups(self):
        print(" The Number of groups for which event extraction failed :: " + str(len(self.group_events_failed)))
        #print(os.listdir('../deeplearning'))
        reprstring=''
        if(self.reprocessMode):
            reprstring='reprocess_'
        failed_log=open(self.opfolder+reprstring+'FailedFileEvents.txt','a')
        for f in self.group_events_failed:
            failed_log.write(f.event_file_group+'\n')
        failed_log.close()


