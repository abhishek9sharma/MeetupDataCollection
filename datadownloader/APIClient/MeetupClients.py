__author__ = 'abhisheksh'
__author__ = 'abhisheksh'



import json
import meetup.api
import  pandas as pd

class MeetUpClients:
    def __init__(self,config=None,configfolder=None):
        self.configfolder=configfolder
        if(self.configfolder is not  None):
            self.configfile=self.configfolder+'/Config/MeetupKeys2.json'
        else:
            self.configfile='Config/MeetupKeys2.json'

        if(config is None):
            self.config=self.ReadDefaultConfig()
        else:
            self.config=config
        self.InitClients()


    def InitClients(self):
        self.clients=[]
        for ks in self.config:
            self.clients.append((ks['AID'],meetup.api.Client(ks['ACCESS_TOKEN'])))


    def ReadDefaultConfig(self):
        with open(self.configfile, 'r') as f:
            config = json.load(f)
            #print(config)
        return config


    def GetMeetUpGroups(self,searchstring,offsetval):
        return self.client.GetGroups(topic=searchstring,page=200,offset=offsetval)


    def GetGroupInfo(self,urlname):
        return self.client.GetGroup({'urlname': urlname})


    def Get_CSV_from_JSON(self,filename,jsondata):
        pd.read_json(json.dumps(jsondata)).to_csv(filename+'.csv',index=False)

    def GetAllTopics(self):
        return self.client.GetTopics()

    def GetAllCategories(self):
        return self.client.GetCategories()

    def GetGroupsbyCategory(self,categoryid,offsetval):
        return self.client.GetGroups(category_id=categoryid,page=200,offset=offsetval)

