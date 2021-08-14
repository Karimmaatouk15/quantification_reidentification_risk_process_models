import os
from enum import Enum
from random import random
import copy
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.objects.process_tree.obj import Operator
from collections import defaultdict
import json
import datetime
from pm4py.algo.conformance.tokenreplay import algorithm as token_replay
from pm4py.visualization.petrinet.util import performance_map
from pm4py.util import constants
from pm4py.util import exec_utils
from pm4py.util import xes_constants
from pm4py.statistics.variants.log import get as variants_get
from pm4py.objects.conversion.wf_net import converter as wf_net_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter


class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_TIMESTAMP_KEY


def assign_leaf_nodes_frequencies(tree, dict1):
    """
    Assigns leaf nodes frequencies
    Parameters
    --------------
    tree
        Process tree
    dict1
        Petri net transition frequency dictionary
    Returns
    -------------
    dict_supplied
        Dictionary process tree leaf nodes with frequency count
    """
    dict_supplied = defaultdict(dict)
    stack = [tree]
    while len(stack) > 0:
        new_tree = stack.pop(0)
        for child in new_tree.children:
            if child.operator is None and not (child in dict_supplied and new_tree in dict_supplied[child]):
                dict_supplied[child][new_tree] = dict1[child.name]
            if len(child.children) > 0:
                stack.append(child)
    return dict_supplied


def assign_non_leaf_nodes_frequencies(tree, dict_copy):
    """
    Assigns non-leaf nodes frequencies
    Parameters
    --------------
    tree
        Process tree
    dict_copy
        Process tree leaf nodes frequency dictionary
    Returns
    -------------
    dict_copy
        Dictionary process tree all nodes with frequency count
    """
    if tree.operator is None:
        return dict_copy[tree][tree.parent]
    elif tree.operator is Operator.XOR:
        if not (tree in dict_copy and tree.parent in dict_copy[tree]):
            dict_copy[tree][tree.parent] = 0
            for child in tree.children:
                dict_copy[tree][tree.parent] = dict_copy[tree][tree.parent] + assign_non_leaf_nodes_frequencies(child,
                                                                                                                dict_copy)
        return dict_copy[tree][tree.parent]
    elif tree.operator is Operator.SEQUENCE or tree.operator is Operator.PARALLEL:
        if tree not in dict_copy and tree.parent not in dict_copy[tree]:
            for child in tree.children:
                new = assign_non_leaf_nodes_frequencies(child, dict_copy)
                if new > 0 and child in dict_copy and tree in dict_copy[child]:
                    dict_copy[tree][tree.parent] = new
        return dict_copy[tree][tree.parent]
    elif tree.operator is Operator.LOOP:
        dict_copy[tree][tree.parent] = assign_non_leaf_nodes_frequencies(tree.children[0],
                                                                         dict_copy) \
                                       - assign_non_leaf_nodes_frequencies(
            tree.children[1], dict_copy)
        return dict_copy[tree][tree.parent]


def get_execution_sequence(tree, dict_copy):
    """
    Gets a trace from a process tree (top-bottom)
    Parameters
    --------------
    tree
        Process tree
    Returns
    -------------
    ex_seq
        Execution sequence
    """
    if tree.operator is None and tree._label is not None:
        return [tree]
    elif tree.operator is None and tree._label is None:
        return [tree]
    elif tree.operator is Operator.XOR:
        eligible = []
        for child in tree.children:
            if dict_copy[child][tree] > 0:
                eligible.append(child)
        if len(eligible) > 0:
            child_chosen = eligible[0]
            dict_copy[child_chosen][tree] = dict_copy[child_chosen][tree] - 1
            new_ret = get_execution_sequence(child_chosen, dict_copy)
            if len(new_ret) == 0:
                dict_copy[child_chosen][tree] = dict_copy[child_chosen][tree] + 1
                return []
            return new_ret
        return []
    elif tree.operator is Operator.SEQUENCE:
        ret = []
        has_empty = False
        for child in tree.children:
            if dict_copy[child][tree] > 0:
                dict_copy[child][tree] = dict_copy[child][tree] - 1
                new_ret = get_execution_sequence(child, dict_copy)
                if len(new_ret) > 0:
                    ret = ret + new_ret
                else:
                    has_empty = True
        if has_empty:
            for child in tree.children:
                dict_copy[child][tree] = dict_copy[child][tree] + 1
            return []
        return ret
    elif tree.operator is Operator.LOOP:
        ret = []
        cont = True
        rep = 0
        while cont:
            cont = False
            if dict_copy[tree.children[0]][tree] > 0:
                dict_copy[tree.children[0]][tree] = dict_copy[tree.children[0]][tree] - 1
                new_ret1 = get_execution_sequence(tree.children[0], dict_copy)
                if len(new_ret1) > 0:
                    ret = ret + new_ret1
                else:
                    dict_copy[tree.children[0]][tree] = dict_copy[tree.children[0]][tree] + 1
                    return []
            if dict_copy[tree.children[1]][tree] > 0:
                r = random()
                if dict_copy[tree.children[0]][tree] > 0:
                    rem_probability = dict_copy[tree][tree.parent] / dict_copy[tree.children[0]][tree]
                    if dict_copy[tree][tree.parent] == 1:  # attempt to add everything in the last trace
                        while dict_copy[tree.children[1]][tree] > 0:
                            if dict_copy[tree.children[0]][tree] > 0:
                                dict_copy[tree.children[1]][tree] = dict_copy[tree.children[1]][tree] - 1
                                new_ret2 = get_execution_sequence(tree.children[1], dict_copy)
                                if len(new_ret2) > 0:
                                    ret = ret + new_ret2
                                else:
                                    dict_copy[tree.children[1]][tree] = dict_copy[tree.children[1]][tree] + 1
                                    return []
                                dict_copy[tree.children[0]][tree] = dict_copy[tree.children[0]][tree] - 1
                                new_ret3 = get_execution_sequence(tree.children[0], dict_copy)
                                if len(new_ret3) > 0:
                                    ret = ret + new_ret3
                                else:
                                    dict_copy[tree.children[0]][tree] = dict_copy[tree.children[0]][tree] + 1
                                    return []

                    else:
                        if r >= rem_probability:
                            rep = rep + 1
                            cont = True
                            dict_copy[tree.children[1]][tree] = dict_copy[tree.children[1]][tree] - 1
                            new_ret2 = get_execution_sequence(tree.children[1], dict_copy)
                            if len(new_ret2) > 0:
                                ret = ret + new_ret2
                            else:
                                dict_copy[tree.children[1]][tree] = dict_copy[tree.children[1]][tree] + 1
                                return []
        return ret
    elif tree.operator is Operator.PARALLEL:
        ret = []
        eligible = []
        for child1 in tree.children:
            if dict_copy[child1][tree] > 0:
                eligible.append(child1)
        has_empty = False
        for child2 in eligible:
            dict_copy[child2][tree] = dict_copy[child2][tree] - 1
            new_ret = get_execution_sequence(child2, dict_copy)
            if len(new_ret) > 0:
                ret = ret + new_ret
            else:
                has_empty = True
        if has_empty:
            for child2 in eligible:
                dict_copy[child2][tree] = dict_copy[child2][tree] + 1
            return []
        return ret


def tree_constrained_traversal_algorithm(tree, dict_copy):
    """
    Generates Execution traces from a process tree
    Parameters
    --------------
    tree
        Process tree
    dict_copy
        Process tree nodes frequency dictionary
    Returns
    -------------
    feasible_traces
        Traces that are generated by the algorithm
    """
    while dict_copy[tree][None] > 0:
        seq = get_execution_sequence(tree, dict_copy)
        dict_copy[tree][None] = dict_copy[tree][None] - 1
        if len(seq) > 0:
            feasible_traces.append(seq)
        else:
            dict_copy[tree][None] = dict_copy[tree][None] + 1
    print(dict_copy)
    print()


from pm4py.objects.log.obj import EventLog, Trace, Event


def transform_to_event_log(traces):
    """
    Transforms execution traces to event log
    Parameters
    --------------
    traces
        traces generated by the constrained traversal algorithm

    Returns
    -------------
    log
        Transformed event log
    """
    activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters, xes_constants.DEFAULT_NAME_KEY)
    timestamp_key = xes_constants.DEFAULT_TIMESTAMP_KEY
    curr_timestamp = 10000000
    log = EventLog()
    count_trace = 0
    for seq in traces:
        trace = Trace()
        count_trace = count_trace + 1
        not_empty = False
        for el in seq:
            if el.label is not None and el.label:
                event = Event()
                event[activity_key] = el.label
                event[timestamp_key] = datetime.datetime.fromtimestamp(curr_timestamp)
                trace.append(event)
                curr_timestamp += 1
                not_empty = True
        if not_empty:
            trace.attributes[activity_key] = count_trace
            log.append(trace)
    return log


def get_petrinet_transitions_frequencies(log, net):
    """
    Gets transitions counts of Petri net including silent transitions
    Parameters
    --------------
    log
        Original Event log that the Petri net is mined from
    net
        Petri net
    Returns
    -------------
    dictionary
        dictionary with the counts of transitions of a Petri net including silent transitions
    """
    dictionary = {}
    parameters = {}
    ht_perf_method = "last"
    activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters, xes_constants.DEFAULT_NAME_KEY)
    timestamp_key = exec_utils.get_param_value(Parameters.TIMESTAMP_KEY, parameters,
                                               xes_constants.DEFAULT_TIMESTAMP_KEY)

    variants_idx = variants_get.get_variants_from_log_trace_idx(log, parameters=parameters)
    variants = variants_get.convert_variants_trace_idx_to_trace_obj(log, variants_idx)

    parameters_tr = {token_replay.Variants.TOKEN_REPLAY.value.Parameters.ACTIVITY_KEY: activity_key,
                     token_replay.Variants.TOKEN_REPLAY.value.Parameters.VARIANTS: variants}

    aligned_traces = token_replay.apply(log, net, initial_marking, final_marking, parameters=parameters_tr)

    element_statistics = performance_map.single_element_statistics(log, net, initial_marking,
                                                                   aligned_traces, variants_idx,
                                                                   activity_key=activity_key,
                                                                   timestamp_key=timestamp_key,
                                                                   ht_perf_method=ht_perf_method)

    for key, value in element_statistics.items():
        dictionary[str(key)] = value['count']
    return dictionary


for filename in os.listdir('chosen_logs'):
    log = xes_importer.apply(os.path.join("chosen_logs", filename))
    net, initial_marking, final_marking = inductive_miner.apply(log=log, variant=inductive_miner.Variants.IMf,
                                                                parameters={
                                                                    inductive_miner.Variants.IM.value.Parameters.NOISE_THRESHOLD: 0.0})
    tree = wf_net_converter.apply(net, initial_marking, final_marking)
    element_stat = get_petrinet_transitions_frequencies(log, net)
    dict_copy = assign_leaf_nodes_frequencies(tree, element_stat)
    assign_non_leaf_nodes_frequencies(tree=tree, dict_copy=dict_copy)
    for sim_n in range(1, 6):
        backup = copy.deepcopy(dict_copy)
        dict_before = {}
        dict_after = {}
        parameters = {}

        feasible_traces = []
        for key, value in backup.items():
            dict_before[str(key)] = str(value)

        json.dump(dict_before,
                  open(os.path.join("simulated_logs",
                                    filename.strip(".xes") + "_dict_before" + str(sim_n) + '_' + ".json"),
                       'w'))
        tree_constrained_traversal_algorithm(tree, backup)
        for key, value in backup.items():
            dict_after[str(key)] = str(value)
        json.dump(dict_after,
                  open(os.path.join("simulated_logs",
                                    filename.strip(".xes") + "_dict_after" + str(sim_n) + '_' + ".json"),
                       'w'))
        log2 = transform_to_event_log(feasible_traces)
        xes_exporter.apply(log2,
                           os.path.join("simulated_logs", 'Simulated_' + str(sim_n) + '_' + filename))

