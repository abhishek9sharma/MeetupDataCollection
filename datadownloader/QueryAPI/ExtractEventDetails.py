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
        except:
            logstr=" Exception Occured  " + str(sys.exc_info()[0]) + " :: for EVENT |" + str(event_id_in)  + "| with group id " + str(group_url_in)
            currmethodtrace.append(logstr)
            print(logstr)

        return (rsvps_event_cnt,currmethodtrace,(event_id_in,group_url_in))


    def ExtractParallell(self,methodname,list_to_process):

        eventpool=mp.Pool(self.num_of_clients)
        #print(len(allgroups_ids_urls))
        processedresults=eventpool.map(methodname,list_to_process)
        eventpool.close()
        eventpool.join()

        return processedresults

    def GetEventDetails(self, allevents_ids_urls_full):
        listtoprocess = zip(allevents_ids_urls_full, range(len(allevents_ids_urls_full)))
        for e in listtoprocess:
            self.GetEventRSVPDetails(e)

        EventRSVPS = self.ExtractParallell(self.GetEventRSVPDetails, listtoprocess)





