__author__ = 'abhisheksh'
__author__ = 'abhisheksh'



import json
import meetup.api
import  pandas as pd
import  os
import  ast
from requests_oauthlib import OAuth2Session

class MeetUpClients:
    def __init__(self, configfolder=None, configfile = 'MeetupKeys2.json'):
        self.configfolder=configfolder
        self.configfile = configfile
        if(self.configfolder is not  None):
            self.configfile=self.configfolder+'/' + configfile
        else:
            configfilepath = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','Config/'+ self.configfile)
            self.configfile=configfilepath

        self.config=self.ReadConfig()
        self.InitClients()


    def InitClients(self):
        self.clients=[]
        for ks in self.config:
            api_obj = meetup.api.Client(self.GetToken(ks))
            if api_obj is not None:
                self.clients.append((ks['AID'], api_obj))
                #self.clients.append((ks['AID'],meetup.api.Client(ks['ACCESS_TOKEN'])))
            else:
                print("Failed to find valid token for client ", ks['AID'])


    def GetToken(self, api_info):
        key = api_info['ACCESS_KEY']
        secret = api_info['ACCESS_SECRET']
        redirect_uri = api_info['REDIRECT_URI']
        code = api_info['CODE']
        token = api_info['TOKEN']
        if token is "":
            try:
                oauth_session = OAuth2Session(key, redirect_uri=redirect_uri)
                token_url = r'https://secure.meetup.com/oauth2/access'
                token = oauth_session.fetch_token(token_url, client_secret=secret, code=code, include_client_id=True)
                api_info['TOKEN'] = token
                return token['access_token']
            except:
                return None

        else:
            return ast.literal_eval(api_info['TOKEN'])['access_token']










    def ReadConfig(self):
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

