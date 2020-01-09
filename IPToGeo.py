#!/usr/bin/python3
# -*- coding: utf-8 -*-

from multiprocessing import Process, Lock, Value
from elasticsearch import Elasticsearch, helpers
from geopy.geocoders import Nominatim
from ip2Region import Ip2Region
import geoip2.database
import os, time, signal
import redis, json,sys

es_host = "172.19.21.200"
re_host = "172.19.21.200"

'''初始化地区解析坐标数据库'''
proc_status = Value('b',False)
print("proc_status",proc_status.value)
City_coordinates_file = "./City_coordinates_file.txt"
if not os.path.isfile(City_coordinates_file):
    with open(City_coordinates_file, "w")as f:
        f.write("")
with open(City_coordinates_file, "r")as f:
    data = f.read()
    if data == "":
        City_coordinates = {}
    else:
        City_coordinates = json.loads(data)

class get_coordinates(object):
    '''地区解析坐标'''
    def __GetLatLng(self,cityname):
        geolocator = Nominatim()
        location2 = geolocator.geocode(cityname,)
        lat = location2.latitude
        lon = location2.longitude
        return {"lat":lat, "lon":lon}

    def __Geoip(self,ip):
        reader = geoip2.database.Reader('./GeoLite2-City.mmdb')
        response = reader.city(ip)
        reader.close()
        return {"lat": response.location.latitude, "lon": response.location.longitude}

    def GetCityCoordinates(self,cityname,Country,ip=None):
        global City_coordinates_file
        global City_coordinates
        if cityname in City_coordinates:
            return City_coordinates[cityname]
        if Country == "中国":
            if cityname[-1] in "市":
                coordinates = self.__GetLatLng(cityname[0:-1])
            else:
                coordinates = self.__GetLatLng(cityname)
            City_coordinates[cityname] = coordinates
            with open(City_coordinates_file,"w")as f:
                f.write(json.dumps(City_coordinates))
            return coordinates
        else:
            coordinates = self.__Geoip(ip)
            City_coordinates[cityname] = coordinates
            with open(City_coordinates_file, "w")as f:
                f.write(json.dumps(City_coordinates))
            return coordinates

def times_run(func):
    """计算运行时间模块"""
    def wrawap(*args, **kwargs):
        int_start = time.time()
        res = func(*args, **kwargs)
        print("耗时{:.2f}".format(time.time() - int_start))
        return res
    return wrawap

class redis_get(object):
    '''redis模块'''
    def __init__(self,host_ip,host_port,dbname=0,passwd=None):
        Pool = redis.ConnectionPool(host=host_ip, port=host_port, db=dbname,max_connections=5)
        self.redir_conn  = redis.Redis(connection_pool=Pool)

    def GetListData(self,list_key,start_data,end_data):
        redis_data = self.redir_conn.lrange(list_key,start_data,end_data)
        return redis_data

    def GetListLen(self,list_key):
        redis_key_len = self.redir_conn.llen(list_key)
        return redis_key_len

    def DelListData(self,list_key,start_data,ent_data=-1):
        return self.redir_conn.ltrim(list_key,start_data,ent_data)

    def redis_set(self,KEY,DATA):
        return self.redir_conn.set(KEY,DATA)

    def redis_get(self,KEY):
        return self.redir_conn.get(KEY)

def request_type(data):
    if ".js" in data:
        return "javascript"
    elif ".css" in data:
        return "css"
    elif ".html" in data:
        return "html"
    else:
        return "backup"

def IPToCitySearch(log_data):
    """IP解析地区"""
    dbFile = "./ip2region.db"
    if (not os.path.isfile(dbFile)) or (not os.path.exists(dbFile)):
        print("[Error]: Specified db file is not exists.")
        return 0
    searcher = Ip2Region(dbFile)
    log_data_list = []
    if isinstance(log_data,list):
        for log in log_data:
            ip = log["remote_addr"]
            data = searcher.memorySearch(ip)
            data_list = data["region"].decode('utf-8').split("|")
            get_con = get_coordinates()
            if data_list[3] != "0":
                City = data_list[3]
            else:
                City = data_list[2]
            #print(City)
            if data_list[0] == "中国" and City != "0":
                City_lon = get_con.GetCityCoordinates(City, data_list[0], ip)
            else:
                City_lon = get_con.GetCityCoordinates("0", data_list[0], ip)
            log["request_type"] = request_type(log["request"])
            log.update({"geoip":{"country_name":data_list[0], "region_name":data_list[2], "city_name":data_list[3],"location":City_lon}})
            log_data_list.append(log)
    searcher.close()
    return log_data_list

def batch_data(logdata):
    """json日志数据写入es"""
    es = Elasticsearch(["%s:9201"% es_host])
    index_name = "nginx-%s" %(time.strftime("%Y-%m-%d"))
    action = ({
        "_index": index_name,
        "_type": "_doc",
        "_source": log_data
    } for log_data in logdata)
    helpers.bulk(es, action)

def proc_stop(signum, frame):
    print("proc_status_stop",proc_status.value)
    print("服务停止中")
    proc_status.value = False
    print("服务停止成功")
    sys.exit(0)

def get_log(lock,proc_id,proc_status):
    '''日志处理出程序'''
    def get_redis_data(redis_conn,redis_key,loop_data):
        log_utf8 = []
        log_data = redis_conn.GetListData(redis_key,0,loop_data)
        for I in log_data:
            log_utf8.append(bytes.decode(I))
        redis_conn.DelListData(redis_key, loop_data)
        return log_utf8

    def get_redis_tmp_data(redis_conn,redis_key,redis_data=None):
        if isinstance(redis_data,list):
            redis_conn.redis_set(redis_key, "||".join(redis_data))
        else:
            redis_data = redis_conn.redis_get(redis_key)
            if redis_data != None and redis_data != "":
                redis_data = bytes.decode(redis_data).split("||")
        if isinstance(redis_data,list):
            log_list = []
            try:
                for I in redis_data:
                    log_list.append(json.loads(I))
            except Exception as exc:
                print(exc)
                print(I)

            batch_data(IPToCitySearch(log_list))
            redis_conn.redis_set(redis_key, "")

    def run_start(redis_conn,redis_key,redis_tmp_key,loop_data=1000):
        reids_len = redis_conn.GetListLen(redis_key)
        print("redis len",reids_len )
        if reids_len > 0:
            lock.acquire()
            redis_data = get_redis_data(redis_conn,redis_key,loop_data)
            lock.release()
            get_redis_tmp_data(redis_conn,redis_tmp_key,redis_data)
            time.sleep(0.01)
        else:
            time.sleep(1)
    print("业务进程：%s  服务启动成功" % proc_id)
    loop_data = 1000
    redis_key = "logstash"
    redis_conn = redis_get(re_host, 6379)
    redis_tmp_key = "logstash-tmp-%s" % proc_id
    get_redis_tmp_data(redis_conn,redis_tmp_key)
    while proc_status.value:
        print(proc_id)
        run_start(redis_conn,redis_key,redis_tmp_key,loop_data)
    else:
        print("业务进程：%s  停止成功" % proc_id)

def es_prc_start():
    p_l = []
    pid = {}
    def p_run(q, proc, proc_status):
        prc = Process(target=get_log, args=(q, proc, proc_status))
        p_l.append(prc)
        prc.start()
        return prc.name

    signal.signal(signal.SIGINT, proc_stop)
    #signal.signal(signal.SIGHUP, proc_stop)
    signal.signal(signal.SIGTERM, proc_stop)
    print("主服务启动成功")
    proc_status.value = True
    q = Lock()
    for proc in range(2):
        p_name = p_run(q, proc, proc_status)
        pid[p_name] = proc
    while proc_status.value:
        time.sleep(5)
        for P in p_l:
            if not P.is_alive():
                pid_int = pid[P.name]
                pid.pop(P.name)
                p_l.remove(P)
                p_name = p_run(q, pid_int, proc_status)
                pid[p_name] = pid_int

if __name__ == "__main__":
    es_prc_start()