__author__ = 'abhisheksh'
from jsonprocessor.TransformationUtil import TransformHelper as thlp
import pandas as pd


class ProcessGroups:

    def __init__(self,groupfilecsv,opfolder):
        self.groupcombinedfile=groupfilecsv
        self.group_pdframe=pd.read_csv(groupfilecsv)
        #self.self.topics_groups_df2=pd.io.json.json_normalize(self.group_pdframe['topics'][0])
        self.groups_df=None
        self.topics_groups_df=None
        self.opfolder=opfolder
        self.thlp=thlp()



    def ConvertAllGroupInfotoCSV(self):
        listDict=[]
        topicList=[]
        for idx,row in self.group_pdframe.iterrows():
            rowdict={}
            for c in self.group_pdframe.columns:
            #for c in ['category']:
                if(c=='topics'):
                    try:
                        topics_grp=eval(row[c])
                        for t in topics_grp:
                            t['groupid']=row['id']
                            #print(t)
                            topicList.append(t)
                    except:
                        pass


                else:
                    data=row[c]
                    try:
                        self.thlp.TransformData(rowdict,data,c)
                    except Exception as e:
                        print(e, "Exception Occured for Gorup with Index " + str(idx))

            listDict.append(rowdict)


        self.groups_df=pd.DataFrame(listDict)
        self.topics_groups_df=pd.DataFrame(topicList)
        #new_groups_all.shape
        print(" Groups Processed " + str(len(listDict)) + "  Topics Found " + str(len(topicList)))
        print(" Shape Transformed from " + str(self.group_pdframe.shape) + " to " + str(self.groups_df.shape))




    def WriteConvertedCSV(self):
        self.groups_df.to_csv(self.opfolder+'/groups_converted.csv',index=False)
        self.topics_groups_df.to_csv(self.opfolder+'/topics_groups_converted.csv',index=False)



