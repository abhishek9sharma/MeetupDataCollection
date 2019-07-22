__author__ = 'abhisheksh'



import json
import meetup.api
import  pandas as pd

class MeetUpClient:
    def __init__(self,config=None):
        if(config is None):
            self.config=self.ReadDefaultConfig()
        else:
            self.config=config
        self.InitClient()


    def InitClient(self):
        self.client =  meetup.api.Client(self.config['ACCESS_TOKEN'])


    def ReadDefaultConfig(self):
        with open('../Config/MeetupKey.json', 'r') as f:
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

