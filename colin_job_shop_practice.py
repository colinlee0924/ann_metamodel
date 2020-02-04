#!/usr/bin/env python2
# -*- coding: utf-8 -*-
#
##----- SysML & SimPy with Job shop ------
# * Author: Colin, Lee
# * Date: Aug 4th, 2019
##------------------------------------------
#
import os
import simpy
import numpy as np
import pandas as pd

#entity
class Order:
    def __init__(self, ID, routing, PT, RT, DD, AV):    #PT: process time; RT: release time; DD: due date
        self.ID = ID
        self.routing = routing
        self.PT = PT
        self.RT = RT
        self.DD = DD
        self.AV = AV

        self.progress = 0

#resource in factory
class Source:
    def __init__(self, fac, order_info = None):
        self.fac = fac
        self.order_info = order_info
        self.orders_list = []

    def initialize(self):
        #reference
        self.env = self.fac.env
        self.queues = self.fac.queues
        #attribute
        #statistics
        self.output = 0

        #initial process
        self.process = self.env.process(self.arrival())

    def arrival(self):
        #get the data of orders
        order_num = self.order_info.shape[0]
        for oorder in range(order_num):
            ID      = self.order_info.loc[oorder, "ID"]
            routing = self.order_info.loc[oorder, "routing"].split(',')
            PT      = [int(i) for i in self.order_info.loc[oorder, "process_time"].split(',')]
            RT      = self.order_info.loc[oorder, "release_time"]
            DD      = self.order_info.loc[oorder, "due_date"]
            AV      = self.order_info.loc[oorder, "arrival_interval"]
            order   = Order(ID, routing, PT, RT, DD, AV)

            self.orders_list.append(order)


        order_num = self.order_info.shape[0]
        for i in range(order_num):
            #TO decide which order arrive first
            indx = np.argmin([o.RT for o in self.orders_list])
            order = self.orders_list[indx]
            self.orders_list.remove(order)
            yield self.env.timeout(order.AV)

            if self.fac.log:
                print("{} order {} release.".format(self.env.now, order.ID))

            self.output += 1
            #send order to queue
            target = order.routing[order.progress]
            # print("{}-->".format(target))
            self.queues[target].order_arrive(order)

            self.fac.update_WIP(1)
            self.fac.print_state("arrival")

class Queue:
    def __init__(self, fac, ID):
        self.fac = fac
        self.ID = ID

    def initialize(self):
        #reference
        self.env = self.fac.env
        self.machine = self.fac.machines[self.ID]
        #attribute
        self.space = []
        #statistics
        #initial process

    def order_arrive(self, order):
        if "idle" in self.machine.state:
            self.machine.process_order(order)
        else:
            self.space.append(order)

    def get_order(self):
        if len(self.space) > 0:
            #get oder in queue
            if self.fac.DP_rule == "FIFO":
                order = self.space[0]
            elif self.fac.DP_rule == "EDD":
                idx = np.argmin([O.DD for O in self.space])
                order = self.space[idx]
            elif self.fac.DP_rule == "SPT":
                idx = np.argmin([O.PT[O.progress] for O in self.space])
                order = self.space[idx]
            #send order to machine
            self.machine.process_order(order)
            #remove order form queue
            self.space.remove(order)

class Machine:
    def __init__(self, fac, ID, num=1):
        self.fac = fac
        self.ID = ID
        self.num = num
        self.processing = None

    def initialize(self):
        #reference
        self.env = self.fac.env
        self.queues = self.fac.queues
        self.sink = self.fac.sink
        #attribute
        if self.num == 2:
            self.state = ["idle", "idle"]
        else:
            self.state = ["idle"]
        #statistics
        #initial process

    def process_order(self, order):
        #change state
        if self.state == ["idle", "idle"]:
            self.state[0] = order
            self.processing = order
        elif self.state[0] == "idle":
            self.state[0] = order
            self.processing = order
        else:
            self.state[-1] = order
            self.processing = order
        #process order
        if self.fac.log:
            print("{} : machine {} start processing order {} - {} progress".format(self.env.now, self.ID, order.ID, order.progress))

        self.process = self.env.process(self.process_order_2())

    def process_order_2(self):
        #processing order for PT mins
        order = self.processing
        PT = order.PT[order.progress]
        yield self.env.timeout(PT)

        if self.fac.log:
            print("{} : machine {} finish processing order {} - {} progress".format(self.env.now, self.ID, order.ID, order.progress))
        #change order state
        order.progress += 1
        #send order to next station
        if order.progress < len(order.routing):
            target = order.routing[order.progress]
            self.queues[target].order_arrive(order)
        else:
            self.sink.complete_order(order)
        #change state
        self.state = ["idle" if x == order else x for x in self.state]
        #get next order in queue
        self.queues[self.ID].get_order()

        self.fac.print_state("{}_Complete".format(self.ID))

class Sink:
    def __init__(self, fac):
        self.fac = fac

    def initialize(self):
        #reference
        self.env = self.fac.env
        #attribute
        #statistics
        self.order_statistic = pd.DataFrame(columns = ["ID", "release_time", "complete_time", "due_date", "flow_time", "tardiness", "lateness"])
        #initial process

    def complete_order(self, order):
        #update factory statistic
        self.fac.throughput += 1
        self.fac.update_WIP(-1)
        #update order statistic
        self.update_order_statistic(order)


        #ternimal condition
        order_num = order_info.shape[0]
        print(order_num, self.fac.throughput)
        if self.fac.throughput >= order_num:
            self.fac.terminal.succeed()
            self.fac.makespan = self.env.now

    def update_order_statistic(self, order):
        ID = order.ID
        RT = order.RT
        complete_time = self.env.now
        DD = order.DD
        flow_time = complete_time - RT
        tardiness = complete_time - DD
        lateness = max(0, tardiness)
        self.order_statistic.loc[ID] = [ID, RT, complete_time, DD, flow_time, tardiness, lateness]

#factory
class Factory:
    def __init__(self, env, log = True):
        self.env = env
        self.log = log
        self.makespan = None

    def initialize(self, order_info, DP_rule):    #DP_rule: dispatching rule
        #build
        self.source = Source(self, order_info)
        self.queues = {"A" : Queue(self, "A"),
                       "B" : Queue(self, "B"),
                       "C" : Queue(self, "C"),
                       "D" : Queue(self, "D")}
        self.machines = {"A" : Machine(self, "A"),
                         "B" : Machine(self, "B"),
                         "C" : Machine(self, "C"),
                         "D" : Machine(self, "D")}
        self.sink = Sink(self)
        #initialize
        self.source.initialize()
        for key, queue in self.queues.items():
            queue.initialize()
        for key, machine in self.machines.items():
            machine.initialize()
        self.sink.initialize()

        #attribute
        self.DP_rule = DP_rule
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
        #terminal event
        self.terminal = self.env.event()

        #initial process
        self.print_state("Initializtion")

    def update_WIP(self, change):
        self.WIP_area += self.WIP*(self.env.now - self.WIP_change_time)
        self.WIP_change_time = self.env.now
        self.WIP += change

    def print_state(self, Etype):
        Q_A = "{}({})".format(len(self.queues["A"].space), [O.ID for O in self.queues["A"].space])
        Q_B = "{}({})".format(len(self.queues["B"].space), [O.ID for O in self.queues["B"].space])
        Q_C = "{}({})".format(len(self.queues["C"].space), [O.ID for O in self.queues["C"].space])
        Q_D = "{}({})".format(len(self.queues["D"].space), [job.ID for job in self.queues["D"].space])

        #Machine_A
        MC_A = self.machines["A"].state
        if "idle" not in MC_A:
            MC_A = "{}".format([self.machines["A"].state[0].ID])
        #     MC_A = "{}".format([self.machines["A"].state[0].ID, self.machines["A"].state[1].ID])
        # elif MC_A == ["idle", "idle"]:
        #     MC_A = self.machines["A"].state
        # else:
        #     MC_A = "{}".format([x if x == "idle" else x.ID for x in MC_A])
        #Machin_B
        MC_B = self.machines["B"].state
        if "idle" not in MC_B:
            MC_B = "{}".format([self.machines["B"].state[0].ID])
        #Machine_C
        MC_C = self.machines["C"].state
        if "idle" not in MC_C:
            MC_C = "{}".format([self.machines["C"].state[0].ID])
        MC_D = self.machines["D"].state
        if "idle" not in MC_D:
            # print(self.machines["D"].state)
            MC_D = "{}".format(self.machines["D"].state[0].ID)

        self.event_record.loc[self.event_ID] = [Etype,
                                                self.env.now,
                                                Q_A, str(MC_A),
                                                Q_B, str(MC_B),
                                                Q_C, str(MC_C),
                                                Q_D, str(MC_D),
                                                self.WIP]
        self.event_ID += 1

    def compute_statistics(self):
        pass


if __name__ == '__main__':
    input_dir = os.getcwd() + '/input_data/'
    env = simpy.Environment()

    # order_info = pd.read_excel("job_shop_practice1.xlsx")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.width', 300)
    pd.set_option('max_colwidth', 100)
    # DP_rule = "EDD"

    #Decide the DP_rule
    FIFO = "FIFO"
    EDD  = "EDD"
    SPT  = "SPT"
    DP_rule = str(input(">>>[FIFO, EDD, SPT] Choose one and key in!! >> "))
    if DP_rule == FIFO:
        order_info = pd.read_excel(input_dir + "job_shop_practice_FIFO.xlsx")
    elif DP_rule == EDD:
        order_info = pd.read_excel(input_dir + "job_shop_practice_EDD.xlsx")
    elif DP_rule == SPT:
        order_info = pd.read_excel(input_dir + "job_shop_practice_SPT.xlsx")

    fac = Factory(env)
    fac.initialize(order_info, DP_rule)

    env.run(until = fac.terminal)

    print("================================================================================================")
    print("dispatching rule: {}".format(DP_rule))
    print(fac.event_record)
    print("================================================================================================")
    print(fac.sink.order_statistic.sort_values(by = "ID"))
    print("========================================================================")
    print("dispatching rule: {}".format(DP_rule))
    print("Average flow time: {}".format(np.mean(fac.sink.order_statistic['flow_time'])))
    print("Average tardiness: {}".format(np.mean(fac.sink.order_statistic['tardiness'])))
    print("Average lateness: {}".format(np.mean(fac.sink.order_statistic['lateness'])))
    print("Makespan = {}".format(fac.makespan))
