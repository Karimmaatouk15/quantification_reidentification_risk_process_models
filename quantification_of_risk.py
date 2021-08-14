from p_privacy_qt.SMS import SMS
from pm4py.objects.log.importer.xes import factory as xes_importer_factory
import os
import pandas as pd
'''
The quantification of risk from the simulated event logs is done using the work done by Majid  Rafiei  et al. in 
https://github.com/m4jidRafiei/privacy_quantification. We use the library 'p-privacy-qt' published by their work. 
'''
if __name__ == '__main__':
    existence_based = False
    measurement_type = "average"
    sensitive = ['Diagnose']
    event_attributes = ['concept:name']
    time_accuracy = "original"
    life_cycle = ['complete', '', 'COMPLETE']
    all_life_cycle = True
    bk_type = 'set'  # Type of Background Knowledge: set,multiset,sequence
    bk_length = 5    # Maximum Background Knowledge Power Size
    multiprocess = True
    mp_technique = 'pool'
    sms = SMS()
    dictionary = {}
    columns = []
    files = []
    columns.append("bk_length")
    for filename in os.listdir('chosen_logs'):
        columns.append("cd_" + filename.replace('.xes', '', 1))
        columns.append("td_" + filename.replace('.xes', '', 1))
        columns.append("uniq_matched_" + filename.replace('.xes', '', 1))
        files.append(filename)

    for column in columns:
        dictionary[column] = []
    for y in range(1, bk_length + 1):
        dictionary['bk_length'].append(y)

    for filename in os.listdir('chosen_logs'):
        column_name = filename.replace('.xes', '', 1)
        log = xes_importer_factory.apply(os.path.join("chosen_logs", filename))
        for x in range(1, bk_length + 1):
            values = [x]
            all_uniques = []
            cd, td, ad, uniq_matched_cases = sms.calc(log, event_attributes, life_cycle, all_life_cycle, sensitive,
                                                      time_accuracy,
                                                      bk_type, measurement_type, x, existence_based,
                                                      multiprocess=multiprocess, mp_technique=mp_technique)
            all_uniques.append(uniq_matched_cases)
            dictionary["cd_" + column_name].append(cd)
            dictionary["td_" + column_name].append(td)
            dictionary["uniq_matched_" + column_name].append(len(uniq_matched_cases))
            values.append(cd)
            values.append(td)
    df = pd.DataFrame(dictionary)
    df.to_csv('./Results/LogName_Sequence.csv')
