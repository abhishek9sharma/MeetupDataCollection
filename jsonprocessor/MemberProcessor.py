__author__ = 'abhisheksh'
from jsonprocessor.TransformationUtil import TransformHelper as thlp
import pandas as pd
import  os
import pandas.io.common


class ProcessSingleGroupMembers:


    def __init__(self,memberfilecsvin,opfolder=None):
        self.member_file_group=memberfilecsvin
        self.opfolder=opfolder
        self.emptyfile = False
        self.exceptionoccured=False
        try:
            self.group_members_org=pd.read_csv(memberfilecsvin)
        except pandas.io.common.EmptyDataError:
            self.emptyfile=True
            self.exceptionoccured=False

        self.csvinfileinfo=memberfilecsvin.split('_')

        if(len(self.csvinfileinfo)>2):
            self.group_id = str(str(self.csvinfileinfo[1]).split('/')[-1])
        else:
            self.group_id = str(str(self.csvinfileinfo[0]).split('/')[-1])
            #self.group_id = str(memberfilecsvin.split('_')[0]).split('/')[-1]

        #self.group_id=str(memberfilecsvin.split('_')[0]).split('/')[-1]
        self.group_members_df_all=None
        self.group_members_topics_df=None
        self.group_members_joined_df=None
        self.thlp=thlp()



    def ProcessSingleGroupMembersInfo(self):
        groupd_member_listDict=[]
        group_members_topics_list=[]
        #group_members_joined_list=[]

        for idx,row in self.group_members_org.iterrows():
            rowdict={}
            for c in self.group_members_org.columns:
            #for c in ['category']:
                if(c=='topics'):
                    #pass
                    try:
                        topics_member=eval(row[c])
                        for t in topics_member:
                            t['member_id']=row['id']
                            #Possible FIX for Integer OVerflow
                            #t['member_id']=str(row['id'])

                            #print(t)
                            group_members_topics_list.append(t)
                    except:
                        print("Exception Occured while processing topics of members with id " + str(row['id']))
                #elif(c in ['joined','visited']):
                #    pass
                else:
                    data=row[c]
                    try:
                        self.thlp.TransformData(rowdict,data,c)
                    except:
                        print("Exception Occured for " + str(idx))

            groupd_member_listDict.append(rowdict)

        #self.group_members_only=pd.DataFrame(groupd_member_listDict)


        self.group_members_df_all=pd.DataFrame(groupd_member_listDict)
        self.group_members_topics_df=pd.DataFrame(group_members_topics_list)

        self.group_members_joined_df=self.group_members_df_all[['id','joined','visited']]
        self.group_members_joined_df['groupid']=self.group_id
        self.group_members_df_all=self.group_members_df_all.drop(['joined','visited'],axis=1)

        print(" Members Processed " + str(len(groupd_member_listDict)) + "  Topics Found " + str(len(group_members_topics_list)))
        print(" Shape Transformed from " + str(self.group_members_org.shape) + " to " + str(self.group_members_df_all.shape))


    def WriteConvertedCSV(self):
        if(self.opfolder is None):
            self.opfolder='../DL/Data/CSVFormat/Members/Members_Groups/'


        self.group_members_df_all.to_csv(self.opfolder+str(self.group_id)+'_members_converted.csv',index=False)
        self.group_members_topics_df.to_csv(self.opfolder+str(self.group_id)+'_group_members_topics.csv',index=False)
        self.group_members_joined_df.to_csv(self.opfolder+str(self.group_id)+'_group_members.csv',index=False)











