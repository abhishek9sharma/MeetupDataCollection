__author__ = 'abhisheksh'
import  json
from bs4 import BeautifulSoup as bs
import time
import  pandas as pd


class TransformHelper:
    def __init__(self):
        pass


    def IsJson(self,data):
        try:
            json_form=json.loads(data)
            #json_form=eval(data)
        except:
            return False
        return True


    def TransformData(self,rowdict,data,colname):
        try:
            if (type(data) == str):
                #data=data.encode('ascii','ignore').decode("utf-8")
                #data = str(data).replace('"','').replace("\'", '"').replace('true','True').replace('False','false')
                data=str(data).replace('"','\'').replace("{'",'{"').replace("",'').replace("\': \'",'": "').replace("\', \'",'", "').replace("\':",'":').replace(", \'",', "').replace("\', \'",'", "').replace("\'}",'"}').replace("'",'')
                data=data.replace('true','True').replace('False','false')
                data=data.replace('\\xa0',' ')


            if ((self.IsJson(data) or type(data)==dict) and colname!='description'):
                # datatrnsfmed=eval(data)
                if(self.IsJson(data)):
                    datatrnsfmed = json.loads(data)
                else:
                    datatrnsfmed=data

                #if(colname=='photo'):
                #    print("debug")

                if(type(datatrnsfmed)==dict):
                    for k in datatrnsfmed.keys():
                        #print(k)
                        datain = datatrnsfmed[k]
                        self.TransformData(rowdict,datain,colname + "." + k)
                else:
                    rowdict[str(colname)] = data

            else:
                if('description'  in colname):
                    #bs(data,"lxml").text.replace('\n','').replace(',','')
                    if(data==data):
                        descformatted=bs(data,"lxml").text.encode('ascii','ignore').decode("utf-8").replace("\"","").strip()
                        rowdict[str(colname)]= descformatted.replace(",","").replace("\n"," ").replace("\r"," ").replace('"','')
                    else:
                        rowdict[str(colname)]= data
                elif any(timeword in colname for timeword in ['created','updated','time','joined','visited']):
                    if(colname=='timezone'):
                         rowdict[str(colname)]= data
                    elif(data==data):
                        descformatted=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(float(data)/1000))
                        rowdict[str(colname)]= descformatted
                    else:
                        rowdict[str(colname)]= data
                else:
                    if (type(data) == str):
                        data=data.replace(",","").replace(";","").replace("\n"," ").replace("\r"," ").replace('"','')
                        data=data.encode('ascii','ignore').decode("utf-8")
                    #if (colname == 'photo'):
                    #       print("debug")
                    rowdict[str(colname)] = data
        except Exception as e:
            print(str(e)+"::" + colname)
            print(data)
            raise

    #def CombineCSV(folder,files_to_combine):
    def CombineCSV(files_to_combine,opfolder):
        combined_pdf=None
        i=0
        for f in files_to_combine:
            try:
                if(i==0):
                    tempdf=pd.read_csv(f)
                    combined_pdf=tempdf
                else:
                    tempdf=pd.read_csv(f)
                    combined_pdf=pd.concat([combined_pdf,tempdf])

                if('reprocess' in f):
                    grp_id=f.split('_')[1]
                else:
                     grp_id=f.split('_')[0].split('/')[-1]
                #print(grp_id)
                tempdf['grpid']=grp_id
                #tempdf.to_csv(mainfolder+'Data/CSVFormat/Members/Members_Groups/'+str(grp_id)+'_Members.csv',index=None,columns=['id','grpid'])
                tempdf.to_csv(opfolder+str(grp_id)+'_Members.csv',index=None,columns=['id','grpid'])


            except Exception as e:
                print("Exception occured for file with name " + str(e) + " for file " + f)
            i+=1
            if(i%100==0):
                elementsloaded=combined_pdf.shape[0]
                combined_pdf=combined_pdf.drop_duplicates()
                rowsdropped=elementsloaded-combined_pdf.shape[0]
                print(" droped " + str(rowsdropped) + " rows")
                #print(" Processing Complete for " + str(i-1) + "groups")

        return combined_pdf







