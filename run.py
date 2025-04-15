# Copyright 2018-2021 Tsinghua DBGroup
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
from LatencyTuning import QueryLoader
from PGUtils import PGRunner
from sqlSample import sqlInfo
import numpy as np
from itertools import count
from math import log
import random
import time
from DQN import DQN,ENV
from TreeLSTM import SPINN
from JOBParser import DB
import copy
import torch
from torch.nn import init
from ImportantConfig import Config

config = Config()

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
pgrunner = PGRunner(config.dbName,config.userName,config.password,config.ip,config.port,isCostTraining=False,latencyRecord = True,latencyRecordFile = "Latency.json")

with open(config.schemaFile, "r") as f:
    createSchema = "".join(f.readlines())

db_info = DB(createSchema)

featureSize = 128

policy_net = SPINN(n_classes = 1, size = featureSize, n_words = 100,mask_size= len(db_info)*len(db_info),device=device).to(device)
target_net = SPINN(n_classes = 1, size = featureSize, n_words = 100,mask_size= len(db_info)*len(db_info),device=device).to(device)
policy_net.load_state_dict(torch.load("99LatencyTuning.pth"))
target_net.load_state_dict(policy_net.state_dict())
target_net.eval()

DQN = DQN(policy_net,target_net,db_info,pgrunner,device)

if __name__=='__main__':
    JOBQueries = QueryLoader(QueryDir=config.testDir)
    for sql in JOBQueries:
        print(sql.filename)

        print("DP")
        time.sleep(0.5)
        pg_cost = sql.getDPlantecy()
        action_this_epi=[]
        env = ENV(sql, db_info, pgrunner, device)
        print("RTOS")
        time.sleep(0.5)
        start = time.time()*1000
        print("start",start)
        for t in count():
            action_list, chosen_action,all_action = DQN.select_action(env,need_random=False)
            left = chosen_action[0]
            right = chosen_action[1]
            env.takeAction(left,right)
            action_this_epi.append((left, right))
            reward, done = env.reward()
            if done:
                end=time.time()*1000
                # print("end",end)
                # my_cost = env.sel.plan2Cost()
                # print("joinPlanTime",end-start)
                print("-----------------------------")
                # print(action_this_epi)
                # print(env.sel.toSql())
                break