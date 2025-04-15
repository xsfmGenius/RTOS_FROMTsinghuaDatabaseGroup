import psycopg2
import json
from math import log
from ImportantConfig import Config

# import logging
# logger = logging.getLogger('LatencyTraining')

config  = Config()
class PGConfig:
    def __init__(self):
        self.keepExecutedPlan =True
        self.maxTimes = 5
        self.maxTime = 300000

LatencyDict = {}
selectivityDict = {}
LatencyRecordFileHandle = None

class PGRunner:
    def __init__(self,dbname = '',user = '',password = '',host = '',port = '',isCostTraining = True,latencyRecord = True,latencyRecordFile = "RecordFile.json"):
        """
        :param dbname:
        :param user:
        :param password:
        :param host:
        :param port:
        :param latencyRecord:-1:loadFromFile
        :param latencyRecordFile:
        """
        self.con = psycopg2.connect(database=dbname, user=user,
                               password=password, host=host, port=port,dsn=None, connection_factory=None, cursor_factory=None)
        self.cur = self.con.cursor()
        self.config = PGConfig()
        self.isLatencyRecord = latencyRecord
        # self.LatencyRecordFileHandle = None
        global LatencyRecordFileHandle
        self.isCostTraining = isCostTraining
        if latencyRecord:
            LatencyRecordFileHandle = self.generateLatencyPool(latencyRecordFile)


    def generateLatencyPool(self,fileName):
        """
        :param fileName:
        :return:
        """
        import os
        import json
        if os.path.exists(fileName):
            f = open(fileName,"r")
            lines = f.readlines()
            for line in lines:
                data = json.loads(line)
                global LatencyDict
                LatencyDict[data[0]] = data[1]
            f = open(fileName,"a")
        else:
            f = open(fileName,"w")
        return f
    def getLatency(self, sql,sqlwithplan):
        """
        :param sql:a sqlSample object
        :return: the latency of sql
        """
        # query = sql.toSql()
        if self.isCostTraining:
            return self.getCost(sql,sqlwithplan)
        global LatencyDict
        # if self.isLatencyRecord:
        #     if sqlwithplan in LatencyDict:
        #         return LatencyDict[sqlwithplan]

        self.cur.execute("set join_collapse_limit = 1;")
        # self.cur.execute("set join_collapse_limit = 100;")
        # self.cur.execute("SET statement_timeout = "+str(int(sql.timeout()))+ ";")
        self.cur.execute("SET statement_timeout =  300000;")
        # self.cur.execute("set max_parallel_workers=1;")
        # self.cur.execute("set max_parallel_workers_per_gather = 1;")
        self.cur.execute("set geqo_threshold = 20;")

        # self.cur.execute("show all;")
        # rows = self.cur.fetchall()
        thisQueryCost = self.getCost(sql,sqlwithplan)
        if thisQueryCost / sql.getDPCost()<10000000:
            try:
                # print("sql",sqlwithplan)
                self.cur.execute("EXPLAIN ANALYZE "+sqlwithplan)
                rows = self.cur.fetchall()
                row = rows[0][0]
                afterCost = float(rows[0][0].split("actual time=")[1].split("..")[1].split(" ")[0])
                # after = float(rows[0][0].split("cost=")[1].split("..")[1].split(" ")[0])
                # self.con.commit()
                # executionTime =float(rows[-1][0].split("Execution Time: ")[1].split(" ")[0])
                # planningTime = float(rows[-2][0].split("Planning Time: ")[1].split(" ")[0])
            # except:
            #     self.con.commit()
            #     afterCost = max(thisQueryCost / sql.getDPCost()*sql.getDPlantecy(),sql.timeout())
            except Exception as e:
                self.con.rollback()
                # logger.info(e)
                print(e)
                # afterCost = max(thisQueryCost / sql.getDPCost() * sql.getDPlantecy(), sql.timeout())
                # afterCost=sql.timeout()
                afterCost = 300000
                # executionTime= 300000
                # planningTime = 300000
        else:
            # afterCost = sql.timeout()
            afterCost = 300000
            # afterCost = max(thisQueryCost / sql.getDPCost()*sql.getDPlantecy(),sql.timeout())
        # print("afterCost", afterCost, "executionTime", executionTime, "planningTime", planningTime)
        # print("afterCost", after, "afterLatency", afterCost)
        # afterCost += 5
        if self.isLatencyRecord:
            LatencyDict[sqlwithplan] =  afterCost
            global LatencyRecordFileHandle
            LatencyRecordFileHandle.write(json.dumps([sqlwithplan,afterCost])+"\n")
            LatencyRecordFileHandle.flush()
        return afterCost

    # def getLatency(self, sql,sqlwithplan):
    #     if self.isCostTraining:
    #         return self.getCost(sql,sqlwithplan)
    #     global LatencyDict
    #     self.cur.execute("COMMIT;")
    #     self.cur.execute("DISCARD ALL;")
    #     self.cur.execute("set join_collapse_limit = 1;")
    #     # self.cur.execute("set join_collapse_limit = 100;")
    #     # self.cur.execute("SET statement_timeout = "+str(int(sql.timeout()))+ ";")
    #     self.cur.execute("SET statement_timeout =  300000;")
    #     # self.cur.execute("set max_parallel_workers=1;")
    #     # self.cur.execute("set max_parallel_workers_per_gather = 1;")
    #     self.cur.execute("set geqo_threshold = 20;")
    #
    #
    #     try:
    #         # print(sqlwithplan)
    #         self.cur.execute("EXPLAIN ANALYZE " + sqlwithplan)
    #         rows = self.cur.fetchall()
    #         # print(rows)
    #         row = rows[0][0]
    #         afterCost = float(rows[0][0].split("actual time=")[1].split("..")[1].split(" ")[0])
    #         # after = float(rows[0][0].split("cost=")[1].split("..")[1].split(" ")[0])
    #         executionTime = float(rows[-1][0].split("Execution Time: ")[1].split(" ")[0])
    #         planningTime = float(rows[-2][0].split("Planning Time: ")[1].split(" ")[0])
    #     except Exception as e:
    #         self.con.rollback()
    #         print(e)
    #         afterCost = 300000
    #         executionTime = 300000
    #         planningTime = 300000
    #     print("afterCost", afterCost, "executionTime", executionTime, "planningTime", planningTime)
    #     # print("afterCost", after, "afterLatency", afterCost)
    #     return afterCost

    def getCost(self,sql,sqlwithplan):
        """
        :param sql: a sqlSample object
        :return: the cost of sql
        """
        self.cur.execute("set join_collapse_limit = 1;")
        # self.cur.execute("set max_parallel_workers=1;")
        # self.cur.execute("set max_parallel_workers_per_gather = 1;")
        self.cur.execute("set geqo_threshold = 20;")
        # self.cur.execute("SET statement_timeout =  100000;")

        self.cur.execute("EXPLAIN "+sqlwithplan)
        rows = self.cur.fetchall()
        row = rows[0][0]
        afterCost = float(rows[0][0].split("cost=")[1].split("..")[1].split(" ")[0])
        self.con.commit()
        return afterCost

    def getDPPlanTime(self,sql,sqlwithplan):
        """
        :param sql: a sqlSample object
        :return: the planTime of sql
        """
        import time
        startTime = time.time()
        cost = self.getCost(sql,sqlwithplan)
        plTime = time.time()-startTime
        return plTime
    def getSelectivity(self,table,whereCondition):
        global selectivityDict
        if whereCondition in selectivityDict:
            return selectivityDict[whereCondition]
        # self.cur.execute("SET statement_timeout = "+str(int(100000))+ ";")
        totalQuery = "select * from "+table+";"
        #     print(totalQuery)

        self.cur.execute("EXPLAIN "+totalQuery)
        rows = self.cur.fetchall()[0][0]
        #     print(rows)
        #     print(rows)
        total_rows = int(rows.split("rows=")[-1].split(" ")[0])

        resQuery = "select * from "+table+" Where "+whereCondition+";"
        # print(resQuery)
        self.cur.execute("EXPLAIN  "+resQuery)
        rows = self.cur.fetchall()[0][0]
        #     print(rows)
        select_rows = int(rows.split("rows=")[-1].split(" ")[0])
        selectivityDict[whereCondition] = -log(select_rows/total_rows)
        #     print(stored_selectivity_fake[whereCondition],select_rows,total_rows)
        return selectivityDict[whereCondition]
