__author__ = 'abhisheksh'
from jsonprocessor.TransformationUtil import TransformHelper as thlp
import pandas as pd
import  os
import  pandas.io.common


class ProcessSingleGroupEvents:


    def __init__(self,eventfilecsvin,opfolder=None):
        self.event_file_group=eventfilecsvin
        self.opfolder=opfolder
        self.emptyfile=False
        self.exceptionoccured = False

        try:
            self.group_events_org=pd.read_csv(eventfilecsvin)
        except pandas.io.common.EmptyDataError:
            self.emptyfile = True
            self.exceptionoccured = True

        self.csvinfileinfo=eventfilecsvin.split('_')

        if(len(self.csvinfileinfo)>2):
            self.group_id = str(self.csvinfileinfo[1]).split('/')[-1]
        else:
            self.group_id=str(self.csvinfileinfo[0]).split('/')[-1]
            #self.group_id=str(eventfilecsvin.split('_')[0]).split('/')[-1]

        self.group_events_df_all=None
        self.thlp=thlp()



    def ProcessGroupEventInfo(self):
        groupd_events_listDict=[]

        for idx,row in self.group_events_org.iterrows():
            rowdict={}
            for c in self.group_events_org.columns:
            #for c in ['category']:
                if(c=='topics'):
                    pass
                else:
                    data=row[c]
                    try:
                        self.thlp.TransformData(rowdict,data,c)
                    except:
                        print("Exception Occured for " + str(idx))

            groupd_events_listDict.append(rowdict)

        #self.group_members_only=pd.DataFrame(groupd_events_listDict)


        self.group_events_df_all=pd.DataFrame(groupd_events_listDict)
        #self.group_members_joined_df=self.group_members_df_all[['id','joined','visited']]
        #self.group_members_joined_df['groupid']=self.group_id
        droplist=['group.created', 'group.group_lat', 'group.group_lon', 'group.join_mode', 'group.name', 'group.urlname', 'group.who']

        for g in droplist:
            if(g in list(self.group_events_df_all.columns)):
                self.group_events_df_all=self.group_events_df_all.drop(g,axis=1)
        print(" Events Processed are " + str(len(groupd_events_listDict)) + "  for group "  + str(self.group_id))
        print(" Shape Transformed from " + str(self.group_events_org.shape) + " to " + str(self.group_events_df_all.shape))


    def WriteConvertedCSV(self):
        if(self.opfolder is None):
            self.opfolder='../DL/Data/CSVFormat/Events/Events_Groups_Individual/'


        self.group_events_df_all.to_csv(self.opfolder+str(self.group_id)+'_events_converted.csv',index=False)











