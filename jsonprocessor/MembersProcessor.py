__author__ = 'abhisheksh'
from jsonprocessor.TransformationUtil import TransformHelper as thlp
import pandas as pd
import  os
from jsonprocessor.MemberProcessor import ProcessSingleGroupMembers as MP
from functools import reduce
import  multiprocessing as mp
import pandas.io.common

class ProcessGroupsMembers:

    def __init__(self,groupfilecsv,memberfolder,opfolder,reprocessMode=False):
        self.memberfolder=memberfolder
        self.opfolder=opfolder
        self.group_pdframe = pd.read_csv(groupfilecsv)
        self.groups_members_files=os.listdir(self.memberfolder)

        self.validgroupids = [str(id) for id in self.group_pdframe['id'].tolist()]
        self.groups_members_files=[f for f in self.groups_members_files if ((f.split('_')[0]  in self.validgroupids)
                                                                          or (f.split('_')[1]  in self.validgroupids)) ]

        self.groups_members_files_sublists=[]
        self.group_members_df_all_combined=None
        self.group_members_topics_df_combined=None
        self.group_members_joined_df_combined=None
        self.members_processed=[]
        self.members_faile=[]
        self.reprocessMode=reprocessMode



    def ProcessSingleGroupMembers(self,f):
        try:
            m_pro=MP(self.memberfolder+f,self.opfolder+'Members_Groups_Individual/')
            if(m_pro.emptyfile==True):
                return m_pro
            else:
                m_pro.ProcessSingleGroupMembersInfo()
                m_pro.WriteConvertedCSV()
                return  m_pro
        except Exception as e:
            print(" Below Exception Occured whiel Processing Events for Group " + str(m_pro.group_id))
            print(e)
            m_pro.exceptionoccured=True
            return m_pro





    def ProcessAllGroups_Members(self,sublistnumber=None):
        members_processed=[]

        '''


        for f in self.groups_members_files:
            m_pro=self.ProcessSingleGroupMembers(f)
            self.members_processed.append(m_pro)
            #print("Debug")

        '''
        if(sublistnumber is not  None):
            cores=mp.cpu_count()-1
            pooltofind_members=mp.Pool(cores)
            self.members_processed=pooltofind_members.map(self.ProcessSingleGroupMembers,self.groups_members_files_sublists[sublistnumber])
            pooltofind_members.close()
            pooltofind_members.join()


        else:
            cores=mp.cpu_count()-1
            pooltofind_members=mp.Pool(cores)
            self.members_processed=pooltofind_members.map(self.ProcessSingleGroupMembers,self.groups_members_files)
            pooltofind_members.close()
            pooltofind_members.join()


        self.group_members_failed= [m for m in self.members_processed if
                                                        (m.emptyfile == True or m.exceptionoccured == True)]

        self.group_members_df_all_combined=pd.concat([m.group_members_df_all for m in self.members_processed if(m.emptyfile==False and m.exceptionoccured==False)])

        self.group_members_joined_df_combined=pd.concat([m.group_members_joined_df for m in self.members_processed])
        self.group_members_topics_df_combined=pd.concat([m.group_members_topics_df for m in self.members_processed])
        self.group_members_topics_df_combined=self.group_members_topics_df_combined.drop_duplicates()

        print("Members Shape before duplicate removal " + str(self.group_members_df_all_combined.shape))
        self.group_members_df_all_combined=self.group_members_df_all_combined.drop_duplicates()
        print("Members Shape after duplicate removal " + str(self.group_members_df_all_combined.shape))

    #TO DO Delete this method
    def ProcessAllGroups_Members_LinearAndPArallel(self,splitnumber):
        members_processed=[]
        '''


        for f in self.groups_members_files:
            m_pro=self.ProcessSingleGroupMembers(f)
            self.members_processed.append(m_pro)
            #print("Debug")

        '''

        splitsize=int(len(self.groups_members_files)/splitnumber)
        self.groups_members_files_sublists=[]
        self.members_processed


        self.groups_members_files_sublists=[self.groups_members_files[i:i+splitsize] for i  in range(0, len(self.groups_members_files), splitsize)]
        self.members_processed=[]


        for sublist in self.groups_members_files_sublists:
            cores=mp.cpu_count()-1
            pooltofind_members=mp.Pool(cores)
            self.members_processed_temp=pooltofind_members.map(self.ProcessSingleGroupMembers,self.groups_members_files)
            pooltofind_members.close()
            pooltofind_members.join()
            self.members_processed=self.members_processed+self.members_processed_temp



        self.group_members_df_all_combined=pd.concat([m.group_members_df_all for m in self.members_processed if(m.emptyfile==False and m.exceptionoccured==False)])
        self.group_members_failed= [m for m in self.members_processed if
                                                        (m.emptyfile == True or m.exceptionoccured == True)]

        self.group_members_joined_df_combined=pd.concat([m.group_members_joined_df for m in self.members_processed])
        self.group_members_topics_df_combined=pd.concat([m.group_members_topics_df for m in self.members_processed])
        self.group_members_topics_df_combined=self.group_members_topics_df_combined.drop_duplicates()

        print("Members Shape before duplicate removal " + str(self.group_members_df_all_combined.shape))
        self.group_members_df_all_combined=self.group_members_df_all_combined.drop_duplicates()
        print("Members Shape after duplicate removal " + str(self.group_members_df_all_combined.shape))



    def WriteConvertedCSV(self,filename=''):
        combfolder='/Members_Groups_Combined/'
        if(self.reprocessMode):
            existingdf_members_converted_combined=pd.read_csv(self.opfolder+combfolder+'members_converted_combined.csv')
            newdf_members_converted_combined=pd.concat([existingdf_members_converted_combined,self.group_members_df_all_combined])
            newdf_members_converted_combined=newdf_members_converted_combined.drop_duplicates()
            newdf_members_converted_combined.to_csv(self.opfolder+combfolder+'members_converted_combined_reprooceseed.csv',index=False)

            existingdf_members_topics_combined=pd.read_csv(self.opfolder+combfolder+'members_topics_combined.csv')
            newdf_members_topics_combined=pd.concat([existingdf_members_topics_combined,self.group_members_topics_df_combined])
            newdf_members_topics_combined=newdf_members_topics_combined.drop_duplicates()
            newdf_members_topics_combined.to_csv(self.opfolder+combfolder+'members_topics_combined_reprooceseed.csv',index=False)

            existingdf_members_groups_combined=pd.read_csv(self.opfolder+combfolder+'members_groups_combined.csv')
            newdf_members_groups_combined=pd.concat([existingdf_members_groups_combined,self.group_members_joined_df_combined])
            newdf_members_groups_combined=newdf_members_groups_combined.drop_duplicates()
            newdf_members_groups_combined.to_csv(self.opfolder+combfolder+'members_groups_combined_reprooceseed.csv',index=False)


        else:
            self.group_members_df_all_combined.to_csv(self.opfolder+combfolder+str(filename)+'members_converted_combined.csv',index=False)
            self.group_members_topics_df_combined.to_csv(self.opfolder+combfolder+str(filename)+'members_topics_combined.csv',index=False)
            self.group_members_joined_df_combined.to_csv(self.opfolder+combfolder+str(filename)+'members_groups_combined.csv',index=False)


    def WriteFailedGroups(self,filename=''):
        print(" The Number of groups for which member extraction failed :: " + str(len(self.group_members_failed)))
        reprstring=''
        if(self.reprocessMode):
            reprstring='reprocess_'
        failed_log=open(self.opfolder+str(filename)+'reprstringFailedFileMembers.txt','a')
        for f in self.group_members_failed:
            failed_log.write(f.member_file_group+'\n')
        failed_log.close()




