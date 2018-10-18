from kafka import KafkaProducer
from kafka.errors import KafkaError

import json, requests, datetime
#from zenoss import Zenoss
import logging
import sys
import time
import os
import configparser

#need to make command line stuff more robust -- e.g. explain options
try:
    sys.argv[1]
except:
    sys.argv = [sys.argv[0], 'default', 'both', 'realtime', 1539849300, 1539849300]#late,gap,both;realtime,cleanup,pointforward,point,range


class loadConfig():
    '''
    library providing config functionality 
    '''
    def __init__(self, section = 'default', cfgFilePath = ""):
        self._url = ''
        self.configSectionName = section
        self.config = self._getConfigDetails(section, cfgFilePath)
       

    def _getConfigDetails(self, section, cfgFilePath):
        '''
        Read 'creds.cfg' configuration file. Default location being in the
        same directory as the python library file & return parameters in
        specific 'section'.
        '''
        #self.log.info('_getConfigDetails; section:%s, cfgFilePath:%s' % (section, cfgFilePath))
        configurations = configparser.ConfigParser()
        if cfgFilePath == "":
            cfgFilePath = os.path.realpath(
                os.path.join(os.getcwd(), os.path.dirname(__file__), 'creds.cfg')
            )
        configurations.read(cfgFilePath)
        if not (section in configurations.sections()):
            raise Exception('Specified configuration section, "%s" not defined in "%s".' % (section, cfgFilePath)) 
        configuration = {item[0]: item[1] for item in configurations.items(section)}
        configuration = self._sanitizeConfig(configuration)
        return configuration


    def _sanitizeConfig(self, configuration):
        '''
        Ensure 'creds.cfg' configuration file has required fields.
        Deal with special fields.
        '''
        #self.log.info('_sanitizeConfig; configuration:%s' %(configuration))
        if not ('url' in configuration):
            raise Exception('Configuration file missing "url" key')
        if not ('username' in configuration):
            raise Exception('Configuration file missing "username" key')
        if not ('password' in configuration):
            pass
            #self.log.error('Configuration file missing "password" key')
        if not ('timeout' in configuration):
            configuration['timeout'] = 3
        else:
            configuration['timeout'] = float(configuration['timeout'])
        if not ('retries' in configuration):
            configuration['retries'] = 3
        else:
            configuration['retries'] = float(configuration['retries'])
        sslVerify = True
        if 'ssl_verify' in configuration:
            if configuration['ssl_verify'].lower() == 'false':
                sslVerify = False
                #requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        configuration['ssl_verify'] = sslVerify
        if 'kafka' not in configuration:
            configuration['kafka']=''
        if 'classes' in configuration:
            configuration['classes']=configuration['classes'].split(',')
        else:
            configuration['classes']=[]
            
        return configuration

configValues=loadConfig(sys.argv[1],('' if 'HOME' not in os.environ else os.environ['HOME']+'/creds.cfg'))

envhosts={configValues.config['url'].split('.')[1]}
scriptmode=sys.argv[2]
scripttiming=sys.argv[3]

if sys.argv[1]=='default':
    devmode=True
else:
    devmode=False
if devmode==True:
    logfile=''
    loglevel='DEBUG'
else:
    logfile='d:'
    loglevel='INFO'#WARNING'
    
logfile+='/tmp/zenossCheckGaps' + sys.argv[1] + scriptmode+  scripttiming+'.log'
producer = KafkaProducer(bootstrap_servers=[configValues.config['kafka']])
deviceClasslist=configValues.config['classes']

#setup logging
class Log(object):

    def __init__(self):
        self.started = False

    def startLogging(self, stream=None, level=None, **kwargs):
        if not self.started:
            if stream is None:
                stream = 'file'

            if level is None:
                level = logging.INFO
            elif level == 'DEBUG':
                level = logging.DEBUG
            elif level == 'WARNING':
                level = logging.WARNING
            elif level == 'ERROR':
                level = logging.ERROR
            elif level == 'CRITICAL':
                level = logging.CRITICAL
            else:
                level = logging.INFO

            #setting logging level for requests package
            logging.getLogger('requests').setLevel(logging.CRITICAL)

            format = "\"{\"timestamp\":\"%(asctime)s\",\"level\":\"%(levelname)s\",\"msg\":%(message)s}\""

            logging.Formatter.converter = time.gmtime

            if stream == 'console':
                logging.basicConfig(stream=sys.stdout,
                                    level=level,
                                    format=format)
            else:
                logging.basicConfig(filename=logfile,
                                    level=level,
                                    format=format)

            self.started = True
            self.info({'logging':'started'})

    def info(self, msg):
        logging.info(json.dumps(msg))
    def debug(self, msg):
        logging.debug(json.dumps(msg))
    def warning(self, msg):
        logging.warning(json.dumps(msg))
    def error(self, msg):
        logging.error(json.dumps(msg))
    def critical(self, msg):
        logging.critical(json.dumps(msg))

log = Log()

log.startLogging('file',loglevel)#file or console

'''Python module fragments to work with the Zenoss JSON API
'''
import ast
import re
import json
import logging
import requests

log = logging.getLogger(__name__) # pylint: disable=C0103
requests.packages.urllib3.disable_warnings()

ROUTERS = {'DeviceRouter': 'device',
           'DeviceManagementRouter': 'devicemanagement'}

class Zenoss(object):
    '''A class that represents a connection to a Zenoss server
    '''
    def __init__(self, host, username, password, ssl_verify=True):
        self.__host = host
        self.__session = requests.Session()
        self.__session.auth = (username, password)
        self.__session.verify = ssl_verify
        self.__req_count = 0

    def __router_request(self, router, method, data=None):
        '''Internal method to make calls to the Zenoss request router
        '''
        if router not in ROUTERS:
            raise Exception('Router "' + router + '" not available.')

        req_data = json.dumps([dict(
            action=router,
            method=method,
            data=data,
            type='rpc',
            tid=self.__req_count)])

        log.debug('Making request to router %s with method %s', router, method)
        uri = '%s/zport/dmd/%s_router' % (self.__host, ROUTERS[router])
        headers = {'Content-type': 'application/json; charset=utf-8'}
        response = self.__session.post(uri, data=req_data, headers=headers)
        self.__req_count += 1
        if response.status_code==500:
            response = self.__session.post(uri, data=req_data, headers=headers)

        # The API returns a 200 response code even whe auth is bad.
        # With bad auth, the login page is displayed. Here I search for
        # an element on the login form to determine if auth failed.
        
        log.debug('Response:'+str(response.content.decode("utf-8")[:4000]))

        if re.search('name="__ac_name"', response.content.decode("utf-8")):
            log.error('Request failed. Bad username/password.')
            raise ZenossException('Request failed. Bad username/password.')
        
        return json.loads(response.content.decode("utf-8"))['result']

    def get_devices_skinny(self, device_class='/zport/dmd/Devices', limit=None, keys=()):
        '''Get a list of all devices.

        '''
        log.info('Getting devices ' + device_class)
        return self.__router_request('DeviceRouter', 'getDevices',
                                     data=[{'uid': device_class, 'keys':keys, 'limit': limit}])




zheaders={"content-type":"application/json","Accept":"application/json"}
zauth=( configValues.config['username'], configValues.config['password'])
startover=True

print('Script mode:',scriptmode,'Script timing:',scripttiming)
log.info({'scriptmode':scriptmode,'scripttiming':scripttiming})

while startover==True:
    startover=False

    devlist={}
    total={}
    timescriptstart=time.time()
    for envhost in envhosts:
        timeloadstart=time.time()
        zurl=configValues.config['url']
        zenoss = Zenoss(zurl, configValues.config['username'], configValues.config['password'],configValues.config['ssl_verify'])

        devlist[envhost]=[]
        toinspect=0
        progress=0
        total[envhost]=0

        for deviceclass in deviceClasslist:
            print('Getting devices',deviceclass,'on',envhost)
            log.info({'Getting devices '+deviceclass+' on':envhost})

            retries=0
            while retries>-1 and retries<5:
                try:
                    all_devices=zenoss.get_devices_skinny(deviceclass,keys=['uid','productionState','collector'])
                    retries=-1
                    startover=False
                except:
                    if retries==0:
                        log.error({"url":zurl})
                    log.error({'ConnectionError':str(sys.exc_info()[0])})
                    retries+=1
                    log.error({'Retry':str(retries)})
                    print('Retry ' + str(retries))
                    all_devices={}
                    all_devices['hash']=0
                    all_devices['devices']={}
                    time.sleep(10)
                    startover=True
            if 'hash' not in all_devices:
                all_devices['hash']=0
                all_devices['devices']={}
                startover=True
                print('Nothing returned in all_devices.')
                log.error({'Nothing returned in':'all_devices'})
            total[envhost]+=int(all_devices['hash'])
            print('Got',all_devices['hash'],'devices from this class. Currently',total,'devices.')
            log.info({'Got ' + str(all_devices['hash']) + ' devices from this class. Currently have this device count':total})
            for device in all_devices['devices']:
                progress += 1
                if device['productionState']>300:
                    devlist[envhost].append([device['uid'],device['collector']])
                    toinspect+=1
        print('Out of',progress,'identified',toinspect,'devices. Device load took',time.time()-timeloadstart,'seconds.')
        log.info({'Out of '+str(progress)+' identified device counts':str(toinspect)})
        log.info({'Device load seconds':str(time.time()-timeloadstart)})
        total[envhost]=toinspect

    devreporting={}
    for envhost in envhosts:
       devreporting[envhost]=[]

    iterationcount=0
    if scripttiming in {'realtime','cleanup'}:
        sleeptime=300-time.time()+int(time.time()/5/60)*5*60
        print('Sleeping',sleeptime,'seconds to wait for the 5 minute interval.',end='')
        log.info({'Sleeping to wait for 5 minute interval seconds':str(sleeptime)})
        while devmode==False and sleeptime>0 and startover==False:
            if sleeptime>5:
                time.sleep(5)
                pass
            else:
                time.sleep(sleeptime)
            print('.',end='')
            sleeptime-=5
        print('')

    if devmode==True and scripttiming in {'realtime','cleanup'}:
        iterationlimit=2
        iterationtarget=time.time()+300
        timeiterationstart=time.time()
    elif scripttiming in {'realtime','cleanup'}:
        targettime=time.time()-10.5*60*60#run until 10:30 GMT
        iterationlimit=(60*60*24-1-targettime+int(targettime/60/60/24)*60*60*24)/60/5
        iterationtarget=time.time()+(60*60*24-290-targettime+int(targettime/60/60/24)*60*60*24)
        print('Set iterations to',iterationlimit,'completion in',iterationlimit*5/60,'hours.')
        print('time',time.time(),'target',iterationtarget)
        log.info({'Set iterations to finish in ' + str(iterationlimit*5/60) + ' hours. Count':str(iterationlimit),'iterationtarget':str(iterationtarget)})
        timeiterationstart=time.time()#timepoint#backpop
    else:
        timeiterationstart=sys.argv[4]
        if scripttiming=='point':
            iterationtarget=sys.argv[4]+1
            iterationlimit=1
        elif scripttiming=='pointforward':
            iterationtarget=time.time()-15*60# can only run up to then due to difference in "present" calculation in real-time
            iterationlimit=10000#arbitrary large for over 30 days
        elif scripttiming=='range':
            iterationtarget=sys.argv[5]
            iterationlimit=10000#arbitrary large for over 30 days...guess could calculate, but why?
        else:
            raise ValueError ('Unknown value for scripttiming')
            
    #for timepoint in {1538730900}:#1538694900}:#1538677800,1538678100,1538678400,1538678700,1538679300,1538679600}:
    while timeiterationstart<iterationtarget and iterationcount<iterationlimit and startover==False:
        for envhost in envhosts:
            toinspect=0
            maxtime=0
            zurl=configValues.config['url']
            zenoss = Zenoss(zurl, configValues.config['username'], configValues.config['password'],False)#configValues.config['ssl_verify'])
            timedorun=time.time()
            progress=0
            if iterationcount!=0:
                print('')
            elif scripttiming=='realtime':
                zparams=dict(start='20d-ago',end='now',series=True,returnset='LAST',metrics=[])
                #zparams=dict(start='1d-ago',end=timeiterationstart,series=True,returnset='LAST',metrics=[])#backpop#if want to go back a full day will need to improve
            else:
                zparams=dict(start=timeiterationstart-20*24*60*60,end=timeiterationstart,series=True,returnset='LAST',metrics=[])
            print('As of',datetime.datetime.now(),'checking what most recent datapoint between',zparams['start'],'and',zparams['end'],'is on',envhost)
            log.info({'On ' + envhost + ' checking what most recent data point is within':zparams['start'],'end':zparams['end']})
            retindex=0
            classDict={}
            collectorDict={}
            if scripttiming in {'realtime'}:
                currenttime=time.time()#timepoint#backpop
            else:
                currenttime=timeiterationstart
            groupedpingcount=[0,0,0,0,0,0,0,0,0,0,0,0]
            posttokafka='validator,collector='+scriptmode+',environment='+envhost+' gapcount=0,gapeendtime='+str(int(currenttime))+ ' ' + str(int(currenttime*1000000000))
            print(posttokafka)
            log.info({'kafka signal post':posttokafka})
            try:
                send = producer.send('lineformat', bytes(posttokafka, 'utf-8'))
            except:
                log.error({'ConnectionError':str(sys.exc_info()[0])})
                
            for device in devlist[envhost]:
                #stophere+=1
                devdataname=device[0][device[0].rindex('/')+1:]
                if 'Cisco' in device[0]:
                    metricname='/avgBusy5_avgBusy5'
                elif 'Microsoft' in device[0]:
                    metricname='/ProcessorTotalProcessorTime_ProcessorTotalProcessorTime'
                else:
                    metricname='/cpu_ssCpuUsedPerCpu'
                zparams['metrics'].extend([dict(metric=devdataname+metricname,rate=False,aggregator='avg',
                                       tags=dict(key=['Devices/'+devdataname]),name=device[1]+device[0]+metricname)])
                progress+=1
                if progress % 100 == 0 or progress == total[envhost]:
                    retries=0
                    while retries>-1 and retries<5:
                        try:
                            resp=requests.post(url=zurl+'api/performance/query', data=json.dumps(zparams), headers=zheaders,auth=zauth,verify=False)
                            pointlist=resp.json()
                            retries=-1
                            startover=False
                        except:
                            if retries==0:
                                log.error({"url":zurl})
                            log.error({'ConnectionError':str(sys.exc_info()[0])})
                            retries+=1
                            log.error({'Retry':str(retries)})
                            print('Retry ' + str(retries))
                            pointlist={}
                            pointlist['endTimeActual']=0
                            pointlist['results']={}
                            time.sleep(10)
                            startover=True
                    if 'endTimeActual' not in pointlist:
                        pointlist['endTimeActual']=0
                        pointlist['results']={}
                        startover=True
                        print('Nothing returned for pointlist.')
                        log.error({'Nothing returned in':'pointlist'})
                        
                    # ---- historical gap list expansion

                    if scriptmode in {'gap','both'}:
                        previouslostpoints=0
                        failoverdetect=0
                        trunchistory=0
                        timewindowend=int(timeiterationstart-15*60)
                        zparams['start']=timewindowend-11*60
                        zparams['end']=timewindowend
                        zparams['returnset']='EXACT'
                        retries=0
                        while retries>-1 and retries<5:
                            try:
                                resp=requests.post(url=zurl+'api/performance/query', data=json.dumps(zparams), headers=zheaders,auth=zauth,verify=False)
                                gaplist=resp.json()  
                                retries=-1
                                startover=False
                            except:
                                if retries==0:
                                    log.error({"url":zurl})
                                log.error({'ConnectionError':str(sys.exc_info()[0])})
                                retries+=1
                                log.error({'Retry':str(retries)})
                                print('Retry ' + str(retries))
                                gaplist={}
                                gaplist['results']={}
                                time.sleep(10)
                                startover=True
                        if 'results' not in gaplist:
                            startover=True
                            gaplist['results']={}
                            print('Nothing returned for gaplist.')
                            log.error({'Nothing returned in':'gaplist'})
                        
                        #print (json.dumps(gaplist,indent=4,sort_keys=True))
                        for results in gaplist['results']:
                            if 'datapoints' in results:
                                if len(results['datapoints'])==1 and results['datapoints'][0]['timestamp']>int(timewindowend-5.5*60): #has to be early enough in the detect window to not be the beginning of a gap
                                    print('Gap Found on',results['metric'],'using',timewindowend)
                                    log.info({'Gap found using ' + str(timewindowend) + ' on':results['metric']})
                                    zparams['start']='3d-ago'
                                    if 'Cisco' in results['metric']:
                                        metricname='/avgBusy5_avgBusy5'
                                    elif 'Microsoft' in results['metric']:
                                        metricname='/ProcessorTotalProcessorTime_ProcessorTotalProcessorTime'
                                    else:
                                        metricname='/cpu_ssCpuUsedPerCpu'
                                    zparams['metrics']=[dict(metric=results['tags']['key'][0][8:]+metricname,rate=False,aggregator='avg',
                                       tags=dict(key=[results['tags']['key'][0]]),name=results['metric'])]
                                    #print (json.dumps(zparams,indent=4,sort_keys=True))

                                    retries=0
                                    while retries>-1 and retries<5:
                                        try:
                                            resp=requests.post(url=zurl+'api/performance/query', data=json.dumps(zparams), headers=zheaders,auth=zauth,verify=False)
                                            retries=-1
                                            startover=False
                                            gapeval=resp.json()
                                        except:
                                            if retries==0:
                                                log.error({"url":zurl})
                                            log.error({'ConnectionError':str(sys.exc_info()[0])})
                                            retries+=1
                                            log.error({'Retry':str(retries)})
                                            print('Retry ' + str(retries))
                                            time.sleep(10)
                                            startover=True
                                    if startover==True or 'results' not in gapeval:
                                        startover=False
                                        print('Nothing returned for gapeval.')
                                        log.error({'Nothing returned in':'gapeval'})
                                        time.sleep(5)
                                    else:
                                        #print (json.dumps(resp.json(),indent=4,sort_keys=True))
                                        lastpoint=len(gapeval['results'][0]['datapoints'])-1
                                        print(lastpoint,' datapoints returned over 3 days.')
                                        log.info({'Datapoints returned over 3 days':str(lastpoint)})
                                        gapseconds=gapeval['results'][0]['datapoints'][lastpoint]['timestamp']-gapeval['results'][0]['datapoints'][lastpoint-1]['timestamp']
                                        lostpoints=int((gapseconds-180)/300)
                                        if failoverdetect<11 and (previouslostpoints<lostpoints+2 and previouslostpoints>lostpoints-2 and lostpoints>72 and sys.argv[1]!='vip1'):
                                            failoverdetect+=1
                                            log.info({'Detected potential secondary change candidate':str(failoverdetect)})
                                            if failoverdetect>10:
                                                trunchistory=timewindowend-30*60
                                                log.info({'Activated secondary change detection':str(trunchistory)})
                                        else:
                                            if failoverdetect<11:
                                                failoverdetect=0
                                                previouslostpoints=lostpoints
                                           
                                        print('Lost',lostpoints,'datapoints in' if lostpoints!=1 else 'datapoint in',gapseconds,'seconds.')
                                        log.info({'Datapoint lost count':str(lostpoints)})
                                        log.info({'Datapoint lost seconds':str(gapseconds)})
                                        metriclongname=gapeval['results'][0]['metric']
                                        nospaceclass=metriclongname[metriclongname.index('/')+19:metriclongname.rindex('devices/')-1].replace(' ','-')
                                        devname=metriclongname[metriclongname.index('devices/')+8:metriclongname.rindex('/')].replace(' ','-')
                                        posttokafka='validator,collector='+metriclongname[0:metriclongname.index('/')]+',class='+nospaceclass+',environment='+envhost+',host='+devname+' gapseconds='+str(
                                            int(gapseconds)) + ',gapcount='+str(lostpoints)+',timepoint='+str(timewindowend)+' ' + str(int(timewindowend*1000000000))
                                        print(posttokafka)
                                        log.info({'kafka summary':posttokafka})
                                        pointstoallocate=lostpoints
                                        posttotalstring=',gaptotalseconds='+str(int(gapseconds)) + ',gaptotalcount='+str(lostpoints)
                                        intervalpoint=timewindowend
                                        while pointstoallocate>-1 and intervalpoint>trunchistory:
                                            intervalpoint=int((timewindowend-180-(lostpoints-pointstoallocate)*5*60)/5/60)*5*60
                                            posttokafka='validator,collector='+metriclongname[0:metriclongname.index(
                                                '/')]+',class='+nospaceclass+',environment='+envhost+',host='+devname+' gapcount='+(
                                                    '1' if pointstoallocate!=0 else '0')+',gapeendtime='+str(timewindowend)+posttotalstring+' ' + str(intervalpoint*1000000000)
                                            print(posttokafka)
                                            if posttotalstring!='':
                                                log.info({'kafka first post':posttokafka})
                                            if True:
                                                try:
                                                    send = producer.send('lineformat', bytes(posttokafka, 'utf-8'))
                                                    pass
                                                except:
                                                    Print('Error posting to kafka.')
                                            pointstoallocate-=1
                                            posttotalstring=''
                                            if pointstoallocate==0:
                                                pointstoallocate=-1

                    # --- historical gap list expansion end

                    currenttime=pointlist['endTimeActual']
                    groupedpingevents=[currenttime-60*5,currenttime-60*10,currenttime-60*15,
                                       currenttime-60*20,currenttime-60*30,currenttime-60*40,
                                       currenttime-60*50,currenttime-60*60,currenttime-60*75,currenttime-60*90]
                    if scriptmode in {'late','both'} or iterationcount==0:
                        for lastpoint in pointlist['results']:
                            if 'datapoints' in lastpoint:#['queryStatus']['status'] not in ['ERROR','WARNING'] 
                                lastpointtime=lastpoint['datapoints'][0]['timestamp']
                                lastTime=lastpointtime
                                if lastTime>maxtime:
                                    maxtime=lastTime

                                eventdelta=(currenttime-lastTime)/60
                                currentClass=devlist[envhost][retindex][1] + ' ' +devlist[envhost][retindex][0][19:devlist[envhost][retindex][0].rindex('devices/')-1]
                                devname=devlist[envhost][retindex][0][devlist[envhost][retindex][0].rindex('/')+1:]
                                if 'Devices/'+devname!=lastpoint['tags']['key'][0]:
                                    startover=True
                                    log.error({'Devices/'+devname+'!=':lastpoint['tags']['key'][0]})
                                    #raise ValueError
                                    print('Devices/'+devname+'!='+lastpoint['tags']['key'][0])
                                if currentClass in classDict:
                                    classDict[currentClass][11] += 1
                                else:
                                    classDict[currentClass]= [0,0,0,0,0,0,0,0,0,0,0,1,eventdelta]
                                if eventdelta<classDict[currentClass][12]:
                                    classDict[currentClass][12]=eventdelta
                                if devlist[envhost][retindex][1] in collectorDict:
                                    collectorDict[devlist[envhost][retindex][1]][11] += 1
                                else:
                                    collectorDict[devlist[envhost][retindex][1]]= [0,0,0,0,0,0,0,0,0,0,0,1]
                                groupitem=0
                                grouphome=False
                                previousgrouptime=currenttime
                                for grouptime in groupedpingevents:
                                    if lastTime>grouptime and lastTime<previousgrouptime:
                                        groupedpingcount[groupitem]+=1
                                        classDict[currentClass][groupitem] += 1
                                        collectorDict[devlist[envhost][retindex][1]][groupitem] += 1
                                        grouphome=True
                                        previousgrouptime=grouptime
                                    groupitem+=1
                                        
                                if grouphome==False:
                                    #d=groupedpingevents[0] -- first list is a day; second run is an hour, won't have old ones
                                    #print(currenttime+90-d,lastTime-d,lastpointtime-d)
                                    groupedpingcount[10]+=1
                                    classDict[currentClass][10] += 1
                                    collectorDict[devlist[envhost][retindex][1]][10] += 1


                                if iterationcount % 1 == 0:
                                    #if not devname.endswith(('.net','.com','.NET','.COM')):
                                    #    devname=devname.replace('.','-')
                                    devname=devname.replace(' ','-')
                                    nospaceclass=currentClass[currentClass.index(' ')+1:].replace(' ','-')
                                    #if scriptrole=='present':
                                    #    lastpointtime+=60*5#move it forward 5 minutes to be consistent with historical
                                    # unfortunately that will set ultra-current datapoints (e.g. <5 minutes) to negative, throwing off the way everything
                                    # else is done, so I need to think about this more. For now if I just set the flag so that pattern is the same for
                                    # the ones flagged, you'll have to use the type of run for that point to determine how to interpret the time values. 
                                    if currenttime-lastpointtime>11*60 or (currenttime-lastpointtime>6*60 and scripttiming!='realtime'):
                                        missedcount='1'
                                    else:
                                        missedcount='0'
                                    #posttokafka=devname+'.'+currentClass[currentClass.rindex(' ')+1:]+'.env.'+ devlist[envhost][retindex][1]+'.validator '+ str(int(currenttime-lastpointtime)) + ' ' + str(int(currenttime))
                                    posttokafka='validator,collector='+devlist[envhost][retindex][1]+',class='+nospaceclass+',environment='+envhost+',host='+devname+' pointage='+str(
                                        int(currenttime-lastpointtime)) + ',pointmissed='+missedcount+',timepoint='+str(currenttime)+' ' + str(int((timeiterationstart-300)*1000000000))
                                    try:
                                        if scriptmode in {'both','late'}:
                                            send = producer.send('lineformat', bytes(posttokafka, 'utf-8'))
                                        pass
                                    except:
                                        Print('Error posting to kafka.')
                                    #print(devlist[envhost][retindex],datetime.datetime.fromtimestamp(lastpointtime).strftime('%c'))
                                    if missedcount=='1':
                                        print(posttokafka)
                                if iterationcount==0:
                                    devreporting[envhost].extend([devlist[envhost][retindex]])
                                    toinspect+=1
                            retindex+=1
                    pointlist={}
                    if scripttiming=='realtime':
                        zparams=dict(start='1d-ago',end='now',series=True,returnset='LAST',metrics=[])
                        #zparams=dict(start='1d-ago',end=timepoint,series=True,returnset='LAST',metrics=[])#seems like wrong indent, but needs to clear metrics=[], but probably needs to be at top#backpop
                    else:
                        zparams=dict(start=timeiterationstart-24*60*60,end=timeiterationstart,series=True,returnset='LAST',metrics=[])
                        if scripttiming=='pointforward':
                            iterationtarget=time.time()
                    
                
            if iterationcount==0:
                print('Out of',progress,'identified',toinspect,'devices reporting specified metric.')
                log.info({'Out of '+str(progress)+ ' identified number of devices reporting specified metric':str(toinspect)})
                devlist[envhost]=devreporting[envhost]
                total[envhost]=toinspect

            if scriptmode in {'late','both'}:
                print('As of',datetime.datetime.now(),'newest datapoint is',(currenttime+time.time()-timedorun-maxtime)/60,'minutes old (',(currenttime-maxtime)/60/60/24,'days)')
                log.info({'Newest datapoint minutes old':str((currenttime+time.time()-timedorun-maxtime)/60)})
                print('Ones within (minutes):')
                log.info({'Ones within:':'minutes'})
                infostring=''
                for i in groupedpingevents:
                    #print(str((i-currenttime)/60)+',',end='')
                    infostring+=str((i-currenttime)/60)+','
                #print('')
                print(infostring)
                log.info({'Bucket time values:':infostring})                
                infostring=''
                for i in groupedpingcount:
                    infostring+=str(i)+','
                print(infostring)
                log.info({'Overall recency counts:':infostring})                

                for classentry in classDict:
                    infostring=''
                    for value in classDict[classentry]:
                        infostring+=str(value)+','
                    print(infostring+classentry)
                    log.info({classentry:infostring})                
                for collectorentry in collectorDict:
                    infostring=''
                    for value in collectorDict[collectorentry]:
                        infostring+=str(value)+','
                    print(infostring+collectorentry)
                    log.info({collectorentry:infostring})  

            print('Time for run :',time.time()-timedorun,'seconds.')
            log.info({'Seconds for run':str(time.time()-timedorun)})

        iterationcount+=1

        #sleeptime=timeiterationstart-time.time()
        sleeptime=-time.time()+int(time.time()/5/60)*5*60
        if iterationcount<iterationlimit and startover==False and scripttiming not in {'pointforward','range'}:    
            if devmode==True:
                sleeptime+=5
            else:
                sleeptime+=300
            print('Sleeping',sleeptime,'seconds',end='')
            log.info({'Sleeping to wait for 5 minute interval seconds':str(sleeptime)})
            while sleeptime>0:
                if sleeptime>5:
                    time.sleep(5)
                else:
                    time.sleep(sleeptime)
                print('.',end='')
                sleeptime-=5
            print('')
        if scripttiming in {'pointforward','range'}:
            timeiterationstart+=300
            zparams=dict(start=timeiterationstart-24*60*60,end=timeiterationstart,series=True,returnset='LAST',metrics=[]) #redundant...need to review how I'm setting this
        else:
            timeiterationstart=time.time()#timepoint#backpop

