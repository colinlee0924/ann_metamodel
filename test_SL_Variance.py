import os
import csv
import flow_shop_SL as fs
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import simpy
from tqdm import tqdm

# #=============================================================
log_dir    = os.getcwd() + '/runs/'
result_dir = os.getcwd() + '/results/'
input_dir = os.getcwd() + '/input_data/'
out_dir   = os.getcwd() + '/assets/'
# # train_dir = out_dir + 'training_data/'
# # test_dir  = out_dir + 'testing_data/'
# dir_list  = [log_dir, result_dir, input_dir, out_dir]#train_dir, test_dir]
#
# for dir in dir_list:
#     if not os.path.exists(dir):
#         os.makedirs(dir)

#=============================================================
class Factory(fs.Factory):
    def __init__(self, env, job_info, AV1, AV2, AV3, DD, PT_table,
                 ST_table, ME_table, DP_rule, log=False):
        super().__init__(env, job_info, AV1,AV2,AV3, DD, PT_table,
                         ST_table, ME_table, DP_rule, log)

#=============================================================
# standard data information
job_info = pd.read_excel(input_dir + "job_information.xlsx")
PT_table = pd.read_excel(input_dir + "PT_table.xlsx")
ST_table = pd.read_excel(input_dir + "ST_table.xlsx")
ME_table = pd.read_excel(input_dir + "ME_table.xlsx")

# parameters set
simulationTime = 1440 * 5
DP_rule_lst = [1, 2, 3] # 1:MST, 2:EDD, 3:FIFO
DP_rule = 1#np.random.choice(DP_rule_lst)
AV_interval = 30#np.random.randint(20, high=40, size=3)
DD_factor   = 2#np.random.randint(2, high=10)
# AV1, AV2, AV3 = np.random.choice([30, 40, 50])

simulationTime = 1440 * 5

#result
throughput_lst = []

for _ in tqdm(range(100)):
    # parameters set
    AV1, AV2, AV3 = np.random.choice([30, 40, 50], size=3)
    # simulation environment
    env = simpy.Environment()
    fac = Factory(env, job_info, AV1,AV2,AV3, DD_factor, PT_table,
                  ST_table, ME_table, DP_rule, log=False)
    fac.initialize()

    # run simulation
    env.run(until = simulationTime)

    # results
    throughput = fac.throughput
    throughput_lst.append(throughput)


    # makespan = fac.env.now
    # avgFT = np.mean(fac.sink.job_statistic['flow_time'])
    # avgTardi = np.mean(fac.sink.job_statistic['tardiness'])
    # avgLate = np.mean(fac.sink.job_statistic['lateness'])
    # avgWIP = fac.WIP_area / fac.env.now
    # # utilization
    # uti_lst = []
    # for key, mc in fac.machines.items():
    #     uti = mc.get_uti()
    #     uti_lst.append(uti)
    # uti_lst = np.array(uti_lst)
    # avgUti = np.mean(uti_lst)

    # print("====================================================================")
    # print("makespan: {}".format(makespan))
    # print("Average folw time: {}".format(avgFT))
    # print("Aveage WIP: {}".format(avgWIP))
    # print("throughput: {}".format(throughput))
    # print("====================================================================")
    # print()

    # # lable set
    # lbl = {'AV1': AV_interval[0], 'AV2': AV_interval[1],
    #        'AV3': AV_interval[2], 'DD_factor': DD_factor,
    #        'DP_rule': DP_rule, 'throughput': throughput,
    #        'avgFT': avgFT, 'avgTardi': avgTardi, 'avgLate': avgLate,
    #        'avgUti': avgUti, 'avgWIP': avgWIP}

    # if repeat >= 1000:
    #     train_labels.append(lbl)
    # else:
    #     test_labels.append(lbl)

# output the results as a dataframe in xlsx
df = pd.DataFrame(throughput_lst)
df.to_excel(result_dir + 'test.xlsx')

# # output the train labels and test labels as csv files
# train_labels = pd.DataFrame(train_labels)
# test_labels  = pd.DataFrame(test_labels)
# train_labels.to_csv(out_dir + 'train_labels.csv', index=0)
# test_labels.to_csv(out_dir + 'test_labels.csv',  index=0)
# test_labels.to_csv(test_dir + 'test_labels.csv',  index=0)
