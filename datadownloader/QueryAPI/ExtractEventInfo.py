__author__ = 'abhisheksh'
import multiprocessing as mp
import  pandas as pd
import sys
import  json


class EventInfoExtractor:

    def __init__(self,meetupclients):
        self.meetupclients=meetupclients
        self.num_of_clients = len(meetupclients)
        self.traceinfo=[]


    #def __init__(self):
    #    pass


    def GetGroupEvents(self,grpinfo):
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
        eventids=[]
        grp_event_info=[]
        try:
            totalevents_grp=mtupcl.GetEvents(group_id=group_id_in,page=1,offset=1,status="past").meta['total_count']
            logstr=str(totalevents_grp)+ " are the Total events present :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
            currmethodtrace.append(logstr)
            print(logstr)

            #if(totalevents_grp>0):
            if(1==1):
                offsetrange=int(totalevents_grp/200)+1

                if(reprocess):
                    logstr= " Reprocessing For   :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                    print(logstr)
                    currmethodtrace.append(logstr)

                else:
                    logstr= " Processing For   :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                    print(logstr)
                    currmethodtrace.append(logstr)

                #Extract All Events for a Group
                for offsetid in range(offsetrange):
                    #curroffset_event_info=mtupcl.GetEvents(group_id=group_id_in,page=200,offset=offsetid).results
                    curroffset_event_info=mtupcl.GetEvents(group_id=group_id_in,page=200,offset=offsetid,status="past").results

                    if(len(curroffset_event_info)>0):
                        grp_event_info+=curroffset_event_info
                    else:
                        logstr= " No events found for offset id  " + str(offsetid)  +" :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                        print(logstr)
                        break

                #print("DEBUG1 ::" + str(grp_event_info))
                #GET ALL EVENTS IDS
                eventids=[e['id'] for e in grp_event_info]
                #print("DEBUG2 ::" + str(eventids))

                #print("DEBUG ::" + len(eventids))

                #WRITE GROUP EVENTS TO CSV
                if(reprocess):
                    pd.read_json(json.dumps(grp_event_info)).to_csv(opfolder+"Data/Events/reprocess_"+str(group_id_in)+'_Events.csv',index=False)
                else:
                    pd.read_json(json.dumps(grp_event_info)).to_csv(opfolder+"Data/Events/"+str(group_id_in)+'_Events.csv',index=False)

                logstr=str(len(grp_event_info))+ " are the Total events extracted  :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                currmethodtrace.append(logstr)
                print(logstr)
            else:
                logstr=str(totalevents_grp)+ " are the Total events extracted  :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
                #currmethodtrace.append(logstr,[group_id_in,group_url_in])


        except:
            logstr=" Exception Occured  " + str(sys.exc_info()[0]) + " :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
            currmethodtrace.append(logstr)
            print(logstr)
            return currmethodtrace,[group_id_in,group_url_in]


        return currmethodtrace,eventids



    def GetEventCountOfGroup(self,grpinfo):
        currmethodtrace=[]
        group_id_in=grpinfo[0][0]
        group_url_in=grpinfo[0][1]
        clinfo=self.meetupclients[int(grpinfo[1]%self.num_of_clients)]
        mtupcl=clinfo[1]
        logstr= " Intialized client : " + str(clinfo[0])+ " :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
        #currmethodtrace=logstr+"\n"
        currmethodtrace.append(logstr)
        print(logstr)

        grp_event_info=[]
        totalevents_grp=0
        try:
            totalevents_grp=mtupcl.GetEvents(group_id=group_id_in,page=200,offset=0,status="past").meta['total_count']
            logstr=str(totalevents_grp)+ " are the Total events present :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
            currmethodtrace.append(logstr)
            print(logstr)
        except:
            logstr=" Exception Occured  " + str(sys.exc_info()[0]) + " :: for GROUP |" + str(group_url_in)  + "| with group id " + str(group_id_in)
            currmethodtrace.append(logstr)
            print(logstr)

        return (totalevents_grp,currmethodtrace,(group_id_in,group_url_in))



    def ExtractParallell(self,methodname,list_to_process):

        eventpool=mp.Pool(self.num_of_clients)
        #print(len(allgroups_ids_urls))
        processedresults=eventpool.map(methodname,list_to_process)
        eventpool.close()
        eventpool.join()

        #EventCounts=[]
        #for i in zip(allgroups_ids_urls,range(len(allgroups_ids_urls))):
        #    EventCounts.append(self.GetEventCountOfGroup(i))

        return processedresults



    def ExtractEventCountsRecursive(self,allgroups_ids_urls_full,opfolder):
        exceptiongroups=[]

        listtoprocess=zip(allgroups_ids_urls_full,range(len(allgroups_ids_urls_full)))
        EventCounts=self.ExtractParallell(self.GetEventCountOfGroup,listtoprocess)

        exceptiongroups=[i[2] for i in EventCounts if("Exception Occured " in "\n".join(i[1]))]
        SuccessfulGroups=[i for i in EventCounts if("Exception Occured " not in "\n".join(i[1]))]

        reprocesscount=0

        while(len(exceptiongroups)>0 and reprocesscount<5):
             listtoprocess=zip(exceptiongroups,range(len(exceptiongroups)))
             reprocessgroups=self.ExtractParallell(self.GetEventCountOfGroup,listtoprocess)
             exceptiongroups=[i[2] for i in reprocessgroups if("Exception Occured " in "\n".join(i[1]))]
             reproces_suceeeded_groups=[i for i in reprocessgroups if("Exception Occured " not in "\n".join(i[1]))]
             SuccessfulGroups=SuccessfulGroups+reproces_suceeeded_groups
             reprocesscount+=1


        foutsg=open(opfolder+"Data/events.csv",'a')
        for sg in SuccessfulGroups:
            foutsg.write(str(sg[2][0])+","+str(sg[0])+"\n")
        foutsg.close()

        fout_ex=open(opfolder+"Data/events_count_failed.csv",'a')
        for fg in exceptiongroups:
            fout_ex.write(fg.write(str(i[2][0])+","+str(i-1)+"\n"))
        fout_ex.close()

        print(" Counts Extracted For Groups " + str(len(SuccessfulGroups)))
        print(" Counts Extraction failed For Groups " + str(len(exceptiongroups)))
        return (SuccessfulGroups,exceptiongroups)



    def ExtractGroupEventsRecursive(self,allgroups_ids_urls_filtered,opfolder):
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
        EventIds=[]


        for sg in subgroups:
            EventInfotraceGrp=[]
            reprocesFalse=[False]*len(sg)
            #EventInfotraceGrp=eventpool.map(GetGroupEvents,zip(sg[0:7],range(7)))
            EventInfotraceGrp=self.ExtractParallell(self.GetGroupEvents,list(zip(sg,range(len(sg)),[opfolder]*len(sg),reprocesFalse)))

            for s in EventInfotraceGrp:
                loginf="\n".join(s[0])
                self.traceinfo.append(loginf+"\n")
                if("Exception Occured " in loginf):
                    grp_id=s[1][0]
                    grp_url=s[1][1]
                    groups_went_into_exception.append((grp_id,grp_url))
                else:
                    EventIds+=s[1]

        EventExcInfoTrace=[]
        EventsSUccReprocess=[]
        groups_went_into_exception_again=[]

        '''
        for rg in zip(groups_went_into_exception,range(len(groups_went_into_exception))):
            EventExcInfoTrace.append(self.GetGroupEvents(rg,reprocess=True))
            for s in EventExcInfoTrace:
                loginf="\n".join(s[0])
                self.traceinfo(loginf+"\n")
                if("Exception Occured " in loginf):
                    grp_id=s[1][0]
                    grp_url=s[1][1]
                    groups_went_into_exception_again.append((grp_id,grp_url))
                else:
                    EventIds+=s[1]
        '''

        reprocesscount=0
        while(len(groups_went_into_exception)>0 and reprocesscount<5):
            lengthoflisttoprocess=len(groups_went_into_exception)
            reprocesTrue=[True]*len(groups_went_into_exception)
            reprocessedEventInfoTrace=self.ExtractParallell(self.GetGroupEvents,zip(groups_went_into_exception,range(lengthoflisttoprocess),[opfolder]*lengthoflisttoprocess,reprocesTrue))
            groups_went_into_exception=[]
            for s in reprocessedEventInfoTrace:
                loginf="\n".join(s[0])
                self.traceinfo.append(loginf+"\n")
                if("Exception Occured " in loginf):
                    grp_id=s[1][0]
                    grp_url=s[1][1]
                    groups_went_into_exception.append((grp_id,grp_url))
                else:
                    EventIds+=s[1]
            reprocesscount+=1



        UniqEvents=set(EventIds)

        logstr= " Total  Events found across " + str(totalgroups_filtered) + " groups are " + str(len(EventIds))
        print(logstr)
        logstr= " Total Uniq Events found across " + str(totalgroups_filtered) + " groupa are " + str(len(UniqEvents))
        print(logstr)
        return (EventIds,self.traceinfo,groups_went_into_exception)






    def TestPar(self,grpinfo):
          #group_url_in=grpinfo[0][1]
          print(grpinfo)
          #return groupi









