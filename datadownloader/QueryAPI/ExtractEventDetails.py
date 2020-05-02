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


    def GetEventRSVPDetails(self, eventinfo):
        currmethodtrace=[]
        event_id_in=eventinfo[0][0]
        group_url_in=eventinfo[0][1]
        clinfo = self.meetupclients[int(eventinfo[1]%self.num_of_clients)]
        mtupcl=clinfo[1]
        logstr= " Intialized client : " + str(clinfo[0])+ " :: for EVENT |" + str(event_id_in)  + "| with group url" + str(group_url_in)
        #currmethodtrace=logstr+"\n"
        currmethodtrace.append(logstr)
        print(logstr)

        grp_event_info=[]
        rsvps_event_cnt = 0
        try:
            #totalevents_grp=mtupcl.GetEvents(group_id=group_id_in,page=200,offset=0,status="past").meta['total_count']
            rsvps_event_cnt = mtupcl.GetGroupEventsAttendance(urlname=group_url_in, id = int(event_id_in), page=200,offset=0,status="past").meta['total_count']
            logstr=str(rsvps_event_cnt)+ " are the Total RSVPS present :: for EVENT |" + str(event_id_in)  + "| with group id " + str(group_url_in)
            currmethodtrace.append(logstr)
            print(logstr)
        except Exception as e:
            logstr=" Exception"+ str(e)+" Occured  " + str(sys.exc_info()[0]) + " :: for EVENT |" + str(event_id_in)  + "| with group id: " + str(group_url_in)
            currmethodtrace.append(logstr)
            print(logstr)

        return (rsvps_event_cnt,currmethodtrace,(event_id_in,group_url_in))

    def GetEventRSVPMemberDetails(self, eventinfo):
        currmethodtrace=[]
        event_id_in=eventinfo[0][0]
        group_url_in=eventinfo[0][1]
        clinfo = self.meetupclients[int(eventinfo[1]%self.num_of_clients)]
        mtupcl=clinfo[1]
        logstr= " Intialized client : " + str(clinfo[0])+ " :: for EVENT |" + str(event_id_in)  + "| with group url" + str(group_url_in)
        #currmethodtrace=logstr+"\n"
        currmethodtrace.append(logstr)
        print(logstr)

        grp_event_info=[]
        
        try:
            #totalevents_grp=mtupcl.GetEvents(group_id=group_id_in,page=200,offset=0,status="past").meta['total_count']
            rsvp_event_members = mtupcl.GetGroupEventsAttendance(urlname=group_url_in, id = event_id_in, page=200,offset=0,status="past")#.meta['total_count']
            logstr=str(len(rsvp_event_members))+ " are the Total RSVPS present :: for EVENT |" + str(event_id_in)  + "| with group id " + str(group_url_in)
            currmethodtrace.append(logstr)
            print(logstr)
        except Exception as e:
            logstr=" Exception"+ str(e)+" Occured  " + str(sys.exc_info()[0]) + " :: for EVENT |" + str(event_id_in)  + "| with group id: " + str(group_url_in)
            currmethodtrace.append(logstr)
            print(logstr)
            return ([],currmethodtrace,(event_id_in,group_url_in))

        return (rsvp_event_members,currmethodtrace,(event_id_in,group_url_in))

    def ExtractParallell(self,methodname,list_to_process):

        eventpool=mp.Pool(self.num_of_clients)
        #print(len(allgroups_ids_urls))
        processedresults=eventpool.map(methodname,list_to_process)
        eventpool.close()
        eventpool.join()

        return processedresults

    def GetEventDetails(self, allevents_ids_urls_full):
        listtoprocess = zip(allevents_ids_urls_full, range(len(allevents_ids_urls_full)))
        with open('logfile','a') as f:
            f.write("eventsprocessed\n")
        
        c = 0
        for e in listtoprocess:
            #self.GetEventRSVPDetails(e)
            member_dict = {}
            event_attended_members = self.GetEventRSVPMemberDetails(e)
            event_id_in=e[0][0]
            group_url_in=e[0][1]
            for member_info in event_attended_members[0]:
                mid = member_info.member["id"]
                #for k in ["member", "rsvp"]:
                member_dict[mid] = {"id":member_info.member["id"], 
                                    "name": member_info.member["name"],
                                    "rspv":member_info.rsvp["response"],
                                    "event_id":event_id_in}
            
            event_attended_members_df =pd.DataFrame.from_dict(member_dict, orient= "index")

            
            event_attended_members_df['event_id'] = event_id_in
            event_attended_members_df.to_csv('event_ttendance.csv', mode='a', header=False)
            with open('logfile','a') as f:
                f.write(str(event_id_in)+","+str(group_url_in)+"\n")
            if c%500==0:
                print(c, " events processed")
            c+=1

        #EventRSVPS = self.ExtractParallell(self.GetEventRSVPDetails, listtoprocess)





