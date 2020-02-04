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
    def __init__(self, ID, jtype, AT, DD, routing, PT):
        self.ID    = ID
        self.jtype = jtype
        self.AT    = AT    #AT: arrival time
        self.DD    = DD    #DD: due date
        self.CT    = None    #CT: complete time

        self.PT       = PT
        self.routing  = routing
        self.progress = 0

#resource in factory
class Source:
    def __init__(self, fac, AV1, AV2, AV3, DD_factor):
        self.fac = fac
        #attribute
        self.AV_interval = {"a": AV1, "b": AV2, "c": AV3}
        self.DD_factor = DD_factor
        self.df = pd.DataFrame(columns = ["ID", "jtype", "arrival_interval", "due_date", "routing", 'PT'])

    def initialize(self):#, OP):
        #reference
        self.env = self.fac.env
        self.queues = self.fac.queues
        # self.OP = OP    #OP: output port, connect to a queue
        #statistics
        self.output = 0
        #initial process
        self.process_A = self.env.process(self.arrival("a"))
        self.process_B = self.env.process(self.arrival("b"))
        self.process_C = self.env.process(self.arrival("c"))


    def arrival(self, jtype):
        while True:
            #wait until job arrive
            arrival_interval = self.AV_interval[jtype]
            arrival_interval = np.random.exponential(arrival_interval)
            yield self.env.timeout(arrival_interval)
            #generate job
            ID      = self.output
            routing = self.fac.routing_dict[jtype].split(',')
            PT      = [int(i) for i in self.fac.PT_dict[jtype].split(',')]
            AT      = self.env.now
            DD      = AT + self.DD_factor * self.fac.avg_pt[jtype]
            job     = Job(ID, jtype, AT, DD, routing, PT)
            # job.routing = routing#.split(',')
            # job.PT      = PT
            self.df.loc[ID] = [ID, jtype, arrival_interval, DD, routing, PT]

            if self.fac.log:
                print("{} job {} release.".format(self.env.now, job.ID))

            self.output += 1
            #send job to next station
            target = job.routing[job.progress]
            self.queues[target].job_arrive(job)
            # self.OP.job_arrive(job)
            self.fac.update_WIP(1)
            '''
            event_trace
            '''
            self.fac.print_state("arrival")

class Queue:
    def __init__(self, fac, ID, DP_rule):
        self.fac = fac
        self.stage = ID
        self.DP_rule = DP_rule    #DP_rule: dispatching rule

    def initialize(self):
        #reference
        self.env = self.fac.env
        self.OP = [self.fac.machines[self.stage]]
        #attribute
        self.space = []
        #statistics
        #initial process

    def MST(self, MC):
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
        # if self.fac.log:
        #     print('MST---')
        return job

    def EDD(self):
        job_index = np.argmin([job.DD for job in self.space])
        job = self.space[job_index]
        # if self.fac.log:
        #     print('EDD---')
        return job

    def SPT(self):
        job_index = np.argmin(job.PT[job.progress] for job in self.space)
        job = self.space[job_index]
        # if self.fac.log:
        #     print('SPT---')
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
                    elif self.DP_rule == 'SPT':
                        job = self.SPT()
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
            elif self.DP_rule == 'SPT':
                job = self.SPT()
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
    def __init__(self, fac, ID):#, mtype, stage):
        self.fac = fac
        # self.stage = stage
        # self.mtype = mtype
        self.ID = ID

    def initialize(self):#, IP, OP):
        #reference
        self.env = self.fac.env
        # self.IP = IP    #IP: input port, connect to a queue
        self.queues = self.fac.queues
        self.sink = self.fac.sink
        # self.OP = OP    #OP: output port, connect to a queue or sink
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
        c1 = True
        if not c1:
            c2 = self.mode.jtype == job.jtype
        else:
            c2 = False
        if c1 or c2:    #don't need setup
                self.state = "processing"
                self.mode = self.space[0]
                self.SPT = self.env.now
                if self.fac.log:    #log message
                    print("{} : machine {} start processing job {}-{}".format(self.env.now, self.ID, self.space[0].ID, self.space[0].jtype))
                #set next event
                self.process = self.env.process(self.process_job())
        else:    #need setup
            self.state = "setup"
            self.SST = self.env.now
            if self.fac.log:    #log message
                print("{} : machine {} start setup {} -> {}".format(self.env.now, self.ID, self.mode.jtype, self.space[0].jtype))
            #set next evnet
            self.process = self.env.process(self.setup())

    def setup(self):
        #setnext event
        stage = 1#self.stage
        ID = self.ID
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
            print("{} : machine {} start processing job {}-{}".format(self.env.now, self.ID, self.space[0].ID, self.space[0].jtype))
        #set next event
        self.process = self.env.process(self.process_job())
        '''
        event trace
        '''
        self.fac.print_state("{}_Setup_OK".format(self.ID))

    def process_job(self):
        job = self.space[0]
        #processing order for PT mins
        # PT = self.fac.PT_table.loc[(self.fac.PT_table["jtype"] == self.space[0].jtype)&\
        #                            (self.fac.PT_table["stage"] == self.stage)]["process_time"].values[0]
        print(len(job.PT), job.progress, job.ID, job.jtype)
        PT = job.PT[job.progress]
        yield self.env.timeout(PT)
        #change job progress
        job.progress += 1
        #complete process
        if self.fac.log:    #log message
            print("{} : machine {} finish processing job {}-{}".format(self.env.now, self.ID, self.space[0].ID, self.space[0].jtype))
        '''
        setup stastic
        '''
        #save process information for gantt
        ST = self.SPT
        FT = self.env.now
        MC = self
        job_now = "j{}-{}".format(self.space[0].ID, self.space[0].jtype)
        self.fac.update_gantt(MC, ST, FT, job_now)

        #for utilization
        self.BT += FT - ST

        #send job to next station
        if job.progress < len(job.routing):
            print(job.progress, len(job.routing))
            print(job.progress < len(job.routing))
            target = job.routing[job.progress]
            self.queues[target].job_arrive(job)
        else:
            self.sink.job_arrive(job)

        self.space = []
        #change state
        self.state = "idle"
        #get next job in queue
        self.queues[self.ID].get_job(self)
        '''
        event trace
        '''
        self.fac.print_state("{}_Complete".format(self.ID))

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
        self.job_statistic = pd.DataFrame(columns = ["ID", "jtype", "arrival_time", "complete_time", "due_date", "flow_time", "tardiness", "lateness"])
        #initial process

    def job_arrive(self, job):
        #update factory statistic
        self.fac.throughput += 1
        self.fac.update_WIP(-1)
        #update job statistic
        job.CT = self.env.now
        self.update_job_statistic(job)
        # #ternimal condition
        # job_num = self.fac.job_info.shape[0]
        # if self.fac.throughput >= job_num:
        #     self.fac.terminal.succeed()

    def update_job_statistic(self, job):
        ID = job.ID
        AT = job.AT
        CT = job.CT
        DD = job.DD
        jtype = job.jtype
        flow_time = CT - AT
        lateness = CT - DD
        tardiness = max(0, lateness)
        self.job_statistic.loc[ID] = [ID, jtype, AT, CT, DD, flow_time, tardiness, lateness]

#factory
class Factory:
    def __init__(self, env, AV1, AV2, AV3, DD, PT_table, ST_table, DP_rule, log = True):
        self.env = env
        self.log = log
        self.AV1 = AV1
        self.AV2 = AV2
        self.AV3 = AV3
        self.DD_factor = DD
        self.PT_table = PT_table    #PT: process time
        self.ST_table = ST_table    #ST: setup time
        # self.ME_table = ME_table    #ME: machine eligibility
        self.DP_rule = DP_rule    #DP_rule: dispatching rule

        self.routing_dict = {"a": 'A,C,B,D',
                             "b": 'A,B,C',
                             "c": 'C,D,B'}
        self.PT_dict = {"a": '7,4,5,9',
                        "b": '6,6,6',
                        "c": '9,12,8'}
        ptA = sum(self.PT_table.loc[(self.PT_table["jtype"] == "A")]["process_time"])
        ptA = sum([int(i) for i in self.PT_dict["a"].split(',')])
        ptB = sum(self.PT_table.loc[(self.PT_table["jtype"] == "B")]["process_time"])
        ptB = sum([int(i) for i in self.PT_dict["b"].split(',')])
        ptC = sum(self.PT_table.loc[(self.PT_table["jtype"] == "C")]["process_time"])
        ptC = sum([int(i) for i in self.PT_dict["c"].split(',')])
        self.avg_pt  = {"a": ptA, "b": ptB, "c": ptC}

    def initialize(self):
        #build
        self.source = Source(self, self.AV1, self.AV2, self.AV3, self.DD_factor)
        self.queues = {"A" : Queue(self, 'A', self.DP_rule),
                       "B" : Queue(self, 'B', self.DP_rule),
                       "C" : Queue(self, 'C', self.DP_rule),
                       "D" : Queue(self, 'D', self.DP_rule)}
        self.machines = {"A" : Machine(self, 'A'),#, 1, 1),
                         "B" : Machine(self, 'B'),#, 1, 1),
                         "C" : Machine(self, 'C'),#, 1, 1),
                         "D" : Machine(self, 'D')}#, 1, 1),}
        self.sink = Sink(self)
        #initialize
        self.source.initialize()#self.queues["Q1"])

        for key, queue in self.queues.items():
            queue.initialize()
        for key, machine in self.machines.items():
            machine.initialize()

        self.sink.initialize()
        #attribute
        #statistics
        self.throughput = 0

        self.WIP = 0
        self.WIP_area = 0
        self.WIP_change_time = 0

        self.event_ID = 0
        self.event_record = pd.DataFrame(columns = ["type",
                                                    "time",
                                                    "queue A", "MC A",
                                                    "queue B", "MC B",
                                                    "queue C", "MC C",
                                                    "queue D", "MC D",
                                                    "WIP"])
        self.gantt_data = {"MC_name"         : [],
                           "start_process" : [],
                           "process_time"  : [],
                           "job"        : []}
        # #terminal event
        # self.terminal = self.env.event()

        self.print_state("Initializtion")

    def update_WIP(self, change):
        self.WIP_area += self.WIP*(self.env.now - self.WIP_change_time)
        self.WIP_change_time = self.env.now
        self.WIP += change

    def print_state(self, Etype):
        Q_A = "{}({})".format(len(self.queues["A"].space), [job.ID for job in self.queues["A"].space])
        Q_B = "{}({})".format(len(self.queues["B"].space), [job.ID for job in self.queues["B"].space])
        Q_C = "{}({})".format(len(self.queues["C"].space), [job.ID for job in self.queues["C"].space])
        Q_D = "{}({})".format(len(self.queues["D"].space), [job.ID for job in self.queues["D"].space])

        MC_A = self.machines["A"].state
        if MC_A != "idle":
            MC_A = "{}".format(self.machines["A"].space[0].ID)
        MC_B = self.machines["B"].state
        if MC_B != "idle":
            MC_B = "{}".format(self.machines["B"].space[0].ID)
        MC_C = self.machines["C"].state
        if MC_C != "idle":
            MC_C = "{}".format(self.machines["C"].space[0].ID)
        MC_D = self.machines["D"].state
        if MC_D != "idle":
            MC_D = "{}".format(self.machines["D"].space[0].ID)

        self.event_record.loc[self.event_ID] = [Etype,
                                                self.env.now,
                                                Q_A, MC_A,
                                                Q_B, MC_B,
                                                Q_C, MC_C,
                                                Q_D, MC_D,
                                                self.WIP]
        self.event_ID += 1

    def update_gantt(self, MC, ST, FT, job_now):
        self.gantt_data['MC_name'].append("M{}".format(MC.ID))
        self.gantt_data['start_process'].append(ST)
        self.gantt_data['process_time'].append(FT-ST)
        self.gantt_data['job'].append(job_now)

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
                if job[-1] == 'a':
                    return colors[1]
                elif job[-1] == 'b':
                    return colors[2]
                else:
                    return colors[3]

        color = [color_define(j, colors) for j in self.gantt_data['job']]
        ax.barh(y = y, width = width, height = 0.5, left = left, color = color, align = 'center', alpha = 0.6, edgecolor='black')
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
    # job_info = pd.read_excel(input_dir + "job_information.xlsx")    # no used
    PT_table = pd.read_excel(input_dir + "PT_table.xlsx")   # should change to new
    ST_table = pd.read_excel(input_dir + "ST_table.xlsx")   # no used, change if need transition time
    # DP_rule = "FIFO"#"MST"

    # Decision case
    AV1 = int(input('>>> The average interarrival time of job type "A" = '))
    AV2 = int(input('>>> The average interarrival time of job type "B" = '))
    AV3 = int(input('>>> The average interarrival time of job type "C" = '))
    DD = float(input('>>> The due date tightness factor(k) = '))
    DP_rule = str(input('>>>[MST, EDD, SPT, FIFO] Choose one and key in!! >> '))

    #build job shop factory
    t0 = time.process_time()
    # print("t0 = {} seconds".format(t0))
    fac = Factory(env, AV1, AV2, AV3, DD, PT_table, ST_table, DP_rule, log = True)
    fac.initialize()
    #run simulation
    # env.run(until = fac.terminal)
    env.run(until = simulationTime)

    fac.source.df.to_excel("job_info.xlsx")
    #print result
    print("====================================================================")
    print("dispatching rule: {}".format(DP_rule))
    print(fac.event_record)
    print("==================================================================================")
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):#fac.sink.job_statistic.shape[1]):
    print(fac.sink.job_statistic.sort_values(by = "ID"))

    print("==================================================================================")
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
