__author__ = 'abhisheksh'
__author__ = 'abhisheksh'
import multiprocessing as mp
import  pandas as pd
import sys
import  json
import time


class MemberInfoExtractor:

    def __init__(self,meetupclients):
        self.meetupclients=meetupclients
        self.num_of_clients = len(meetupclients)
        self.traceinfo=[]


    #def __init__(self):
    #    pass


    def GetGroupMembers(self,grpinfo):
        currmethodtrace=[]
        group_id_in=grpinfo[0][0]
        group_url_in=grpinfo[0][1]
        opfolder=grpinfo[2]
        reprocess=grpinfo[3]
        clinfo=self.meetupclients[int(grpinfo[1]%self.num_of_clients)]
        mtupcl=clinfo[1]

        if(reprocess):
            logstr= " Intialized client Reprocess Mode : " + str(clinfo[0])+ " :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
        else:
            logstr= " Intialized client : " + str(clinfo[0])+ " :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
        #currmethodtrace=logstr+"\n"
        currmethodtrace.append(logstr)
        print(logstr)

        grp_member_info=[]
        try:
            totalmembers_grp=mtupcl.GetMembers(group_id=group_id_in,page=1,offset=1).meta['total_count']
            logstr=str(totalmembers_grp)+ " are the Total members present :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
            currmethodtrace.append(logstr)
            print(logstr)

            #if(totalevents_grp>0):
            if(1==1):
                offsetrange=int(totalmembers_grp/200)+1

                if(reprocess):
                    logstr= " Reprocessing For   :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                    print(logstr)
                    currmethodtrace.append(logstr)

                else:
                    logstr= " Processing For   :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                    print(logstr)
                    currmethodtrace.append(logstr)
                    #Extract All MEMBERS for a Group
                for offsetid in range(offsetrange):
                    curroffset_member_info=mtupcl.GetMembers(group_id=group_id_in,page=200,offset=offsetid).results
                    if(len(curroffset_member_info)>0):
                        grp_member_info+=curroffset_member_info
                    else:
                        logstr= " No members found for offset id  " + str(offsetid)  +" :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                        print(logstr)
                        break


                #GET MEMBERS IDS

                memberids=[g['id'] for g in grp_member_info]
                #WRITE GROUP MEMBERS TO CSV
                if(reprocess):
                    pd.read_json(json.dumps(grp_member_info)).to_csv(opfolder+"Data/Members/reprocess_"+str(group_id_in)+'_Members.csv',index=False)
                else:
                    pd.read_json(json.dumps(grp_member_info)).to_csv(opfolder+"Data/Members/"+str(group_id_in)+'_Members.csv',index=False)

                logstr=str(len(grp_member_info))+ " are the Total members extracted  :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                currmethodtrace.append(logstr)
                print(logstr)
            else:
                logstr=str(totalmembers_grp)+ " are the Total members extracted  :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                currmethodtrace.append(logstr)


        except:
            logstr=" Exception Occured  " + str(sys.exc_info()[0]) + "for GROUP #" + str(group_url_in)  + "# with group id #" + str(group_id_in)
            currmethodtrace.append(logstr)
            print(logstr)
            return (currmethodtrace,[group_id_in,group_url_in])



        return (currmethodtrace,memberids)






    def ExtractParallell(self,methodname,list_to_process):

        mpool=mp.Pool(self.num_of_clients)
        #print(len(allgroups_ids_urls))
        processedresults=mpool.map(methodname,list_to_process)
        mpool.close()
        mpool.join()

        #EventCounts=[]
        #for i in zip(allgroups_ids_urls,range(len(allgroups_ids_urls))):
        #    EventCounts.append(self.GetEventCountOfGroup(i))

        return processedresults






    def ExtractGroupMembersRecursive(self,allgroups_ids_urls_filtered,opfolder):
        #SPLIT GROUP DATA TO PROCESS IN A SEQ+PARALLEL MODE

        #numtoslice=7
        self.traceinfo=[]
        keysatdisposal=len(self.meetupclients)
        totalgroups_filtered=len(allgroups_ids_urls_filtered)
        chunksize=int((totalgroups_filtered-1)/(keysatdisposal))+1
        subgroups=[allgroups_ids_urls_filtered[m:m+chunksize] for m in range(0,totalgroups_filtered,chunksize)]
        logstr= "Apprx Size of each sub group or chunk:: " + str(chunksize)
        #logstr =logstr+ " \n "+" NUmber of  sub groups " + str()
        self.traceinfo.append(logstr)
        print(logstr)
        groups_went_into_exception=[]
        MemberIds=[]


        for sg in subgroups:
            MemberInfoTraceGrp=[]
            reprocesFalse=[False]*len(sg)
            #EventInfotraceGrp=eventpool.map(GetGroupEvents,zip(sg[0:7],range(7)))
            MemberInfoTraceGrp=self.ExtractParallell(self.GetGroupMembers,list(zip(sg,range(len(sg)),[opfolder]*len(sg),reprocesFalse)))
            print ("Parallel Execution Complete")


            for s in MemberInfoTraceGrp:
                loginf="\n".join(s[0])
                self.traceinfo.append(loginf+"\n")
                if("Exception Occured " in loginf):
                    grp_id=s[1][0]
                    grp_url=s[1][1]
                    groups_went_into_exception.append((grp_id,grp_url))
                else:
                    MemberIds+=s[1]



        reprocesscount=0
        while(len(groups_went_into_exception)>0 and reprocesscount<20):
            print("SLEEPING AS EXCEPTION OCCURED")
            time.sleep(30)
            lengthoflisttoprocess=len(groups_went_into_exception)
            reprocesTrue=[True]*len(groups_went_into_exception)
            reprocessedEventInfoTrace=self.ExtractParallell(self.GetGroupMembers,zip(groups_went_into_exception,range(lengthoflisttoprocess),[opfolder]*lengthoflisttoprocess,reprocesTrue))
            groups_went_into_exception=[]
            for s in reprocessedEventInfoTrace:
                loginf="\n".join(s[0])
                self.traceinfo.append(loginf+"\n")
                if("Exception Occured " in loginf):
                    grp_id=s[1][0]
                    grp_url=s[1][1]
                    groups_went_into_exception.append((grp_id,grp_url))
                else:
                    MemberIds+=s[1]
            reprocesscount+=1



        UniqMembers=set(MemberIds)

        logstr= " Total  Members found across " + str(totalgroups_filtered) + " groups are " + str(len(MemberIds))
        print(logstr)
        logstr= " Total Uniq Members found across " + str(totalgroups_filtered) + " groupa are " + str(len(MemberIds))
        print(logstr)
        #print(self.traceinfo)
        return (MemberIds,UniqMembers,self.traceinfo,groups_went_into_exception)






    def TestPar(self,grpinfo):
          #group_url_in=grpinfo[0][1]
          print(grpinfo)
          #return groupi









