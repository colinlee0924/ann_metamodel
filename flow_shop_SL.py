# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 11:00:31 2019

@author: cimlab
"""

import os
import time
import simpy
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

np.random.seed(999)

#entity
class Job:
    def __init__(self, ID, jtype, AT, DD):
        self.ID = ID
        self.jtype = jtype
        self.AT = AT    #AT: arrival time
        self.CT = None    #CT: complete time
        self.DD = DD    #DD: due date

#resource in factory
class Source:
    def __init__(self, fac, job_info, AV1, AV2, AV3, DD_factor):
        self.fac = fac
        self.AV_interval = {"A": AV1, "B": AV2, "C": AV3}
        self.DD_factor = DD_factor
        self.df = pd.DataFrame(columns = ["ID", "jtype", "arrival_interval", "due_date"])

    def initialize(self, OP):
        #reference
        self.env = self.fac.env
        self.OP = OP    #OP: output port, connect to a queue
        #attribute
        #statistics
        self.output = 0
        #initial process
        self.process_A = self.env.process(self.arrival("A"))
        self.process_B = self.env.process(self.arrival("B"))
        self.process_C = self.env.process(self.arrival("C"))


    def arrival(self, jtype):
        while True:
            #wait until job arrive
            arrival_interval = self.AV_interval[jtype]
            yield self.env.timeout(arrival_interval)
            #generate job
            ID = self.output
            AT    = self.env.now
            DD    = AT + self.DD_factor * self.fac.avg_pt[jtype]
            job   = Job(ID, jtype, AT, DD)
            self.df.loc[ID] = [ID, jtype, arrival_interval, DD]

            if self.fac.log:
                print("{} job {} release.".format(self.env.now, job.ID))

            self.output += 1
            #send job to next station
            self.OP.job_arrive(job)
            self.fac.update_WIP(1)
            '''
            event_trace
            '''

class Queue:
    def __init__(self, fac, stage, DP_rule):
        self.fac = fac
        self.stage = stage
        self.DP_rule = DP_rule    #DP_rule: dispatching rule

    def initialize(self, OP):
        #reference
        self.env = self.fac.env
        self.OP = OP
        #attribute
        self.space = []
        #statistics
        #initial process

    def MST(self, MC):
        if self.fac.log:
            print('MST---')

        if MC.mode == None:
            job = np.random.choice(self.space)
        else:
            ST_list = []
            for j in self.space:
                _from = MC.mode.jtype
                to = j.jtype
                if _from == to:
                    ST_list.append(0)
                else:
                    ST = self.fac.ST_table.loc[(self.fac.ST_table["stage"] == self.stage)&\
                                               (self.fac.ST_table["from"]  == _from)&\
                                               (self.fac.ST_table["to"]    == to)]["setup_time"].values[0]
                    ST_list.append(ST)
            job_index = np.argmin(ST_list)
            job = self.space[job_index]
        return job

    def EDD(self):
        if self.fac.log:
            print('EDD---')
        job_index = np.argmin([job.DD for job in self.space])
        job = self.space[job_index]
        return job

    def dispatch(self, MC = None):
        if MC == None:
            for MC in self.OP:
                if len(self.space) <= 0:
                    break
                if MC.state == "idle":
                    if self.DP_rule == 'MST':
                        job = self.MST(MC)
                    elif self.DP_rule == 'EDD':
                        job = self.EDD()
                    else:
                        job = self.space[0]

                    if job != None:
                        MC.job_accept(job)
                        self.space.remove(job)
        else:
            if self.DP_rule == 'MST':
                job = self.MST(MC)
            elif self.DP_rule == 'EDD':
                job = self.EDD()
            else:
                job = self.space[0]

            if job != None:
                MC.job_accept(job)
                self.space.remove(job)


    def job_arrive(self, job):
        self.space.append(job)
        idle_machine = sum([1 if MC.state == "idle" else 0 for MC in self.OP])
        if idle_machine > 0:
            self.dispatch()

    def get_job(self, MC):
        if len(self.space) > 0:
            self.dispatch(MC)

class Machine:
    def __init__(self, fac, stage, mtype, ID):
        self.fac = fac
        self.stage = stage
        self.mtype = mtype
        self.ID = ID

    def initialize(self, IP, OP):
        #reference
        self.env = self.fac.env
        self.IP = IP    #IP: input port, connect to a queue
        self.OP = OP    #OP: output port, connect to a queue or sink
        #attribute
        self.space = []     #the job on machine put in space
        self.state = "idle"    # state is one of "idle", "setup", "processing"
        self.mode = None    #mode record the last processing job for checking the setup time
        #statistics
        self.SST = 0    #SST: start setup time
        self.SPT = 0    #SPT: start process time
        self.BT  = 0
        #initial process

    def job_accept(self, job):    #all job sould pass this operation not going stright to setup or process state
        self.space.append(job)
        #check setup requirement
        c1 = self.mode == None
        if not c1:
            c2 = self.mode.jtype == job.jtype
        else:
            c2 = False
        if c1 or c2:    #don't need setup
                self.state = "processing"
                self.mode = self.space[0]
                self.SPT = self.env.now
                if self.fac.log:    #log message
                    print("{} : machine {}-{} start processing job {}".format(self.env.now, self.stage, self.ID, self.space[0].ID))
                #set next event
                self.process = self.env.process(self.process_job())
        else:    #need setup
            self.state = "setup"
            self.SST = self.env.now
            if self.fac.log:    #log message
                print("{} : machine {}-{} start setup {} -> {}".format(self.env.now, self.stage, self.ID, self.mode.jtype, self.space[0].jtype))
            #set next evnet
            self.process = self.env.process(self.setup())

    def setup(self):
        #setnext event
        stage = self.stage
        _from = self.mode.jtype
        to = self.space[0].jtype
        setup_time = self.fac.ST_table[(self.fac.ST_table["stage"] == stage)&\
                                       (self.fac.ST_table["from"]  == _from)&\
                                       (self.fac.ST_table["to"]    == to)]["setup_time"].values[0]
        yield self.env.timeout(setup_time)
        #complete setup
        self.mode = self.space[0]
        '''
        setup stastic
        '''
        #save process information for gantt
        ST = self.SST
        FT = self.env.now
        MC = self
        job = "setup {} -> {}".format(_from, to)
        self.fac.update_gantt(MC, ST, FT, job)

        #process job
        self.state = "processing"
        self.SPT = self.env.now
        if self.fac.log:    #log message
            print("{} : machine {}-{} start processing job {}".format(self.env.now, self.stage, self.ID, self.space[0].ID))
        #set next event
        self.process = self.env.process(self.process_job())
        '''
        event trace
        '''

    def process_job(self):
        #processing order for PT mins
        PT = self.fac.PT_table.loc[(self.fac.PT_table["jtype"] == self.space[0].jtype)&\
                                   (self.fac.PT_table["stage"] == self.stage)]["process_time"].values[0]
        yield self.env.timeout(PT)
        #complete process
        if self.fac.log:    #log message
            print("{} : machine {}-{} finish processing job {}".format(self.env.now, self.stage, self.ID, self.space[0].ID))
        '''
        setup stastic
        '''
        #save process information for gantt
        ST = self.SPT
        FT = self.env.now
        MC = self
        job = "j{} - {}".format(self.space[0].ID, self.space[0].jtype)
        self.fac.update_gantt(MC, ST, FT, job)

        #for utilization
        self.BT += FT - ST

        #send job to next station
        self.OP.job_arrive(self.space[0])
        self.space = []
        #change state
        self.state = "idle"
        #get next job in queue
        self.IP.get_job(self)
        '''
        event trace
        '''

    def get_uti(self):
        busyTime = 0
        if self.state == "processing":
            busyTime = (self.env.now - self.SPT) + self.BT
        else:
            busyTime = self.BT
        uti = busyTime / self.env.now
        return uti

class Sink:
    def __init__(self, fac):
        self.fac = fac

    def initialize(self):
        #reference
        self.env = self.fac.env
        #attribute
        #statistics
        self.job_statistic = pd.DataFrame(columns = ["ID", "arrival_time", "complete_time", "due_date", "flow_time", "tardiness", "lateness"])
        #initial process

    def job_arrive(self, job):
        #update factory statistic
        self.fac.throughput += 1
        self.fac.update_WIP(-1)
        #update job statistic
        job.CT = self.env.now
        self.update_job_statistic(job)
        #ternimal condition
        job_num = self.fac.job_info.shape[0]
        # if self.fac.throughput >= job_num:
        #     self.fac.terminal.succeed()

    def update_job_statistic(self, job):
        ID = job.ID
        AT = job.AT
        CT = job.CT
        DD = job.DD
        flow_time = CT - AT
        lateness = CT - DD
        tardiness = max(0, lateness)
        self.job_statistic.loc[ID] = [ID, AT, CT, DD, flow_time, tardiness, lateness]

#factory
class Factory:
    def __init__(self, env, job_info, AV1, AV2, AV3, DD, PT_table, ST_table, ME_table, DP_rule, log = True):
        self.env = env
        self.log = log
        self.AV1 = AV1
        self.AV2 = AV2
        self.AV3 = AV3
        self.DD_factor = DD
        self.job_info = job_info    #job_info: job information
        self.PT_table = PT_table    #PT: process time
        self.ST_table = ST_table    #ST: setup time
        self.ME_table = ME_table    #ME: machine eligibility
        self.DP_rule = DP_rule    #DP_rule: dispatching rule

        ptA = sum(self.PT_table.loc[(self.PT_table["jtype"] == "A")]["process_time"])
        ptB = sum(self.PT_table.loc[(self.PT_table["jtype"] == "B")]["process_time"])
        ptC = sum(self.PT_table.loc[(self.PT_table["jtype"] == "C")]["process_time"])
        self.avg_pt  = {"A": ptA, "B": ptB, "C": ptC}

    def initialize(self):
        #build
        self.source = Source(self, self.job_info, self.AV1, self.AV2, self.AV3, self.DD_factor)
        self.queues = {"Q1" : Queue(self, 1, self.DP_rule),
                       "Q2" : Queue(self, 2, self.DP_rule),
                       "Q3" : Queue(self, 3, self.DP_rule)}
        self.machines = {"M1-1" : Machine(self, 1, 1, 1),
                         "M1-2" : Machine(self, 1, 1, 2),
                         "M1-3" : Machine(self, 1, 2, 3),
                         "M2-1" : Machine(self, 2, 1, 1),
                         "M2-2" : Machine(self, 2, 2, 2),
                         "M2-3" : Machine(self, 2, 2, 3),
                         "M3-1" : Machine(self, 3, 1, 1),
                         "M3-2" : Machine(self, 3, 1, 2),
                         "M3-3" : Machine(self, 3, 2, 3),}
        self.sink = Sink(self)
        #initialize
        self.source.initialize(self.queues["Q1"])

        self.queues["Q1"].initialize([self.machines["M1-1"], self.machines["M1-2"], self.machines["M1-3"]])
        self.queues["Q2"].initialize([self.machines["M2-1"], self.machines["M2-2"], self.machines["M2-3"]])
        self.queues["Q3"].initialize([self.machines["M3-1"], self.machines["M3-2"], self.machines["M3-3"]])

        self.machines["M1-1"].initialize(self.queues["Q1"], self.queues["Q2"])
        self.machines["M1-2"].initialize(self.queues["Q1"], self.queues["Q2"])
        self.machines["M1-3"].initialize(self.queues["Q1"], self.queues["Q2"])
        self.machines["M2-1"].initialize(self.queues["Q2"], self.queues["Q3"])
        self.machines["M2-2"].initialize(self.queues["Q2"], self.queues["Q3"])
        self.machines["M2-3"].initialize(self.queues["Q2"], self.queues["Q3"])
        self.machines["M3-1"].initialize(self.queues["Q3"], self.sink)
        self.machines["M3-2"].initialize(self.queues["Q3"], self.sink)
        self.machines["M3-3"].initialize(self.queues["Q3"], self.sink)

        self.sink.initialize()
        #attribute
        #statistics
        self.throughput = 0

        self.WIP = 0
        self.WIP_area = 0
        self.WIP_change_time = 0
        '''
        self.event_ID = 0
        self.event_record = pd.DataFrame(columns = ["type",
                                                    "time",
                                                    "queue A", "MC A",
                                                    "queue B", "MC B",
                                                    "queue C", "MC C",
                                                    "WIP"])
        '''
        self.gantt_data = {"MC_name"         : [],
                           "start_process" : [],
                           "process_time"  : [],
                           "job"        : []}
        #terminal event
        self.terminal = self.env.event()
        '''
        self.print_state("Initializtion")
        '''
    def update_WIP(self, change):
        self.WIP_area += self.WIP*(self.env.now - self.WIP_change_time)
        self.WIP_change_time = self.env.now
        self.WIP += change
    '''
    def print_state(self, Etype):
        Q_A = "{}({})".format(len(self.queues["A"].space), [O.ID for O in self.queues["A"].space])
        Q_B = "{}({})".format(len(self.queues["B"].space), [O.ID for O in self.queues["B"].space])
        Q_C = "{}({})".format(len(self.queues["C"].space), [O.ID for O in self.queues["C"].space])

        MC_A = self.machines["A"].state
        if MC_A != "idle":
            MC_A = "{}".format(self.machines["A"].state.ID)
        MC_B = self.machines["B"].state
        if MC_B != "idle":
            MC_B = "{}".format(self.machines["B"].state.ID)
        MC_C = self.machines["C"].state
        if MC_C != "idle":
            MC_C = "{}".format(self.machines["C"].state.ID)

        self.event_record.loc[self.event_ID] = [Etype,
                                                self.env.now,
                                                Q_A, MC_A,
                                                Q_B, MC_B,
                                                Q_C, MC_C,
                                                self.WIP]
        self.event_ID += 1
    '''
    def update_gantt(self, MC, ST, FT, job):
        self.gantt_data['MC_name'].append("M{}-{}".format(MC.stage, MC.ID))
        self.gantt_data['start_process'].append(ST)
        self.gantt_data['process_time'].append(FT-ST)
        self.gantt_data['job'].append(job)

    def draw_gantt(self, save = None):
        fig, ax = plt.subplots(1,1)
        #set color list
        # colors = list(mcolors.CSS4_COLORS.keys())
        # np.random.shuffle(colors)
        colors = list(mcolors.TABLEAU_COLORS.keys())
        #draw gantt bar
        y = self.gantt_data['MC_name']
        width = self.gantt_data['process_time']
        left = self.gantt_data['start_process']
        def color_define(job, colors):
            if len(job) == 12:
                return 'black'
            else:
                if job[-1] == 'A':
                    return colors[1]
                elif job[-1] == 'B':
                    return colors[2]
                else:
                    return colors[3]

        color = [color_define(j, colors) for j in self.gantt_data['job']]
        ax.barh(y = y, width = width, height = 0.5, left = left, color = color, align = 'center', alpha = 0.6)
        #put the text on
        for i in range(len(self.gantt_data['MC_name'])):
            text_x = self.gantt_data['start_process'][i] + self.gantt_data['process_time'][i]/2
            text_y = self.gantt_data['MC_name'][i]
            text = self.gantt_data['job'][i]
            if len(text) >=10:
                text = ""
            ax.text(text_x, text_y, text, verticalalignment='center', horizontalalignment='center')
        #figure setting
        ax.set_xlabel("time (mins)")
        ax.set_xticks(np.arange(0, self.env.now+1, 20))
        ax.set_ylabel("Machine")
        ax.set_title("hybrid flow shop - gantt")

        if save is not None:
            fig.savefig(save)
        else:
            try:
                return fig
            except:
                plt.show()

    '''
    def print_schduling_data(self):
        df = pd.DataFrame(columns = ["job ID", "step", "MC ID", "start time", "end time"])
        for i in range(len(self.gantt_data["MC_id"])):
            order_id = self.gantt_data["order_id"][i]
            step = self.gantt_data["order_progress"][i]
            MC_id = self.gantt_data["MC_id"][i]
            ST = self.gantt_data["start_process"][i]
            ET = self.gantt_data["start_process"][i] + self.gantt_data["process_time"][i]
            df.loc[i] = [order_id, step, MC_id, ST, ET]
        with pd.option_context('display.max_rows', None, 'display.max_columns', df.shape[1]):
            print(df)
    '''

if __name__ == '__main__':
    #prepare the I/O path
    input_dir = os.getcwd() + '/input_data/'
    output_dir = os.getcwd() + '/results/'
    dir_list = [input_dir, output_dir]
    for dir in dir_list:
        if not os.path.exists(dir):
            os.makedirs(dir)
    #environment
    env = simpy.Environment()
    #parameter
    simulationTime = 100 #1440 * 5
    job_info = pd.read_excel(input_dir + "job_information.xlsx")
    PT_table = pd.read_excel(input_dir + "PT_table.xlsx")
    ST_table = pd.read_excel(input_dir + "ST_table.xlsx")
    ME_table = pd.read_excel(input_dir + "ME_table1.xlsx")
    # DP_rule = "FIFO"#"MST"

    # Decision case
    AV1 = int(input('>>> The average interarrival time of job type "A" = '))
    AV2 = int(input('>>> The average interarrival time of job type "B" = '))
    AV3 = int(input('>>> The average interarrival time of job type "C" = '))
    DD = float(input('>>> The due date tightness factor(k) = '))
    DP_rule = str(input('>>>[MST, EDD, FIFO] Choose one and key in!! >> '))

    #build job shop factory
    t0 = time.process_time()
    # print("t0 = {} seconds".format(t0))
    fac = Factory(env, job_info, AV1, AV2, AV3, DD, PT_table, ST_table, ME_table, DP_rule, log = False)#True)
    fac.initialize()
    #run simulation
    # env.run(until = fac.terminal)
    env.run(until = simulationTime)

    fac.source.df.to_excel("job_info.xlsx")
    #print result
    '''
    print("====================================================================")
    print("dispatching rule: {}".format(DP_rule))
    print(fac.event_record)
    print("====================================================================")
    with pd.option_context('display.max_rows', None, 'display.max_columns', fac.sink.job_statistic.shape[1]):
        print(fac.sink.job_statistic.sort_values(by = "ID"))
    '''
    print("====================================================================")
    print("throughput: {}".format(fac.throughput))
    print("makespan: {}".format(fac.env.now))
    print("Average flow time: {}".format(np.mean(fac.sink.job_statistic['flow_time'])))
    print("Average tardiness: {}".format(np.mean(fac.sink.job_statistic['tardiness'])))
    print("Average lateness: {}".format(np.mean(fac.sink.job_statistic['lateness'])))
    print("Aveage WIP: {}".format(fac.WIP_area/fac.env.now))
    print("====================================================================")
    uti_lst = []
    for key, mc in fac.machines.items():
        uti = mc.get_uti()
        uti_lst.append(uti)
        print("Utilization of {}: {}".format(key, uti))
    uti_lst = np.array(uti_lst)
    print("Average utilization: {}".format(np.mean(uti_lst)))
    print("====================================================================")

    cpu_time = time.process_time() - t0
    print("Cost {} seconds".format(cpu_time))


    '''
    print("schduling data")
    fac.print_schduling_data()
    print("====================================================================")
    '''
    fac.draw_gantt(output_dir + "{}.png".format(DP_rule))
    # plt.show()
