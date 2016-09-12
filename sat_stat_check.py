#!/usr/bin/env python
# Copyright 2016 PLanet Labs Inc.


import requests
import json
import pandas as pd
import pickle
import collections
import textwrap
import math
import itertools
from collections import defaultdict


BASE_URL = 'https://planet-labs-url.com/'
MAJORITY_SATS = 0.5


def flatten(d, parent_key='', sep=':'):
    '''
    Given a nested dictionaries, returns a flattened version.

    Example:
    Input -> {'a': 1, 'c': {'a': 2, 'b': {'x': 5, 'y' : 10}}, 
              'd': [1, 2, 3]}

    Output -> {'a': 1, 'c_a': 2, 'c_b_x': 5, 'd': [1, 2, 3], 
               'c_b_y': 10}
    '''

    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_sat_state(hw_id):
    '''
    Given a satellites hardware ID, returns it's state paramater
    values.
    '''

    res = requests.get(BASE_URL + 'mission_control/sat_details/{}/'.format(hw_id))
    res.raise_for_status()
    return json.loads(res.json()['state_data'])


def get_data():
    # get list of satellites
    res = requests.get(BASE_URL + 'mission_control/sat_list/')
    hw_ids = []
    for sat in res.json():

        if (sat['status'] != 'inactive'
            and sat['mode'] != 'retired'):

            hw_ids.append(sat['hw_id'])

    all_sat_states = {hw_id : get_sat_state(hw_id)
                      for hw_id in hw_ids}

    # get list of keys (columns in dataframe)
    keys = set()

    for hw_id in all_sat_states:
        flat_stat_states = flatten(all_sat_states[hw_id], sep=':')
        
        for keychain in flat_stat_states:
            keys.add(keychain)

    # initiate pandas data
    data = pd.DataFrame(columns=keys, index=hw_ids)

    # take all data from all sats and add to dataframe SAYING FOR NUMBER LINE, HWID IN DICT
    for idx, hw_id in enumerate(all_sat_states):
        flat_stat_states = flatten(all_sat_states[hw_id], sep=':')
        data.loc[hw_id] = pd.Series(flat_stat_states)
    
    #data.to_csv('/home/steven/git/ops-metrics/data_analysis/reports/test.csv')
    num_sats = len(hw_ids)

    return data, num_sats


def count_keychains(data, crit_num_sats):
    '''
    Collects statistics on state paramaters for all satellites.
    '''

    # loop over keychain columns
    keychains = []
    counts = []
    sats = []
    ignored_keys = []
    metadata_com = [];
    ''' ignore fields '''
    ignore_fields = set()
    ignore_fields = ["list", "of", "ignored", "fields"]

    for keychain in data.columns:
        if keychain in ignore_fields:
            ignored_keys.append(keychain)
        else:
            count = data[keychain].count()

            sat = list(data[data[keychain].notnull() == True].index)
            sat = ', '.join(sat)

            keychains.append(keychain)
            counts.append(count)
            sats.append(sat)

    # sort based on count
    zipped = zip(counts, keychains, sats)
    zipped.sort()
    counts, keychains, sats = zip(*zipped)


    report = '\n***************************************\n\n'

    for count, keychain, sat in zip(counts, keychains, sats):
        if count >= crit_num_sats:
            sats_temp = [list(x.split(', ')) for x in sats]
            sats = list(set(list(itertools.chain.from_iterable(sats_temp))))
            not_sats = [x for x in list(set(sats)) if x not in sat.split(', ')]
            if not_sats:
                report += '\n{0}\nnum sats: {1}\n'.format(
                           keychain.replace(":","."), count)
                report += 'minority sats without this:\n'
                report += textwrap.fill('{}\n'.format(
                    ', '.join(not_sats)),50)
                report += '\n'
            else: 
                metadata_com.append(keychain.replace(':','.'))

        else:
            report += '{0}\nnum sats: {1}\n'.format(
                keychain.replace(":","."), count)
            for sats_hwids in textwrap.wrap(sat, 50):
                report += sats_hwids + '\n'
            report += '\n'

    report += "\n\nMetadata all sats have in common:\n"
    report +="{}\n".format('\n'.join(metadata_com))

    report += '\n***************************************\n\n'

    report += 'Ignored Fields:\n'
    for key in ignored_keys:
        report += key.replace(":",".")  + '\n'

    report += '\n***************************************\n\n'

    return report


def value_analysis(data):
    msg = 'Sat Stat Report\n\n'
    msg += "Value Analysis\n\n"
    msg += "Number of sats being considered: {}\n".format(len(data.index.values))

    value_fields = ["list", "of", "fields", "to", "compare", "values", "of"]

    # Determines which fields are bools or strings 
    value_bools = []
    value_str = []
    value_array = []

    for entry in value_fields:
        if entry in data:
            if data[entry].any() == True or data[entry].any() == False:
                value_bools.append(entry)
            elif type(data[entry][0])==list:
                value_array.append(entry)
            else:
                value_str.append(entry)

    for entry in value_bools:
         # finding opts for a bool
        trues = data[entry][data[entry]==True].index.values
        falses = data[entry][data[entry]==False].index.values
        nans = data[entry][data[entry]!=True][data[entry]!=False].index.values

        if len(trues):
            msg += "\n{} : True\n".format(entry.replace(":","."))
            msg += "num sats: {}\n".format(len(trues))
            msg += textwrap.fill("      {}\n\n".format(', '.join(trues)),50)
        if len(falses):
            msg += "\n{} : False\n".format(entry.replace(":","."))
            msg += "num sats: {}\n".format(len(falses))
            msg += textwrap.fill("      {}\n\n".format(', '.join(falses)),50)
        if len(nans):
            msg += "\n{} : None\n".format(entry.replace(":","."))
            msg += "num sats: {}\n".format(len(nans))
            msg += textwrap.fill("      {}\n\n".format(', '.join(nans)),50)
        if len(nans) or len(trues) or len(falses):
           msg += "\n\n"

    for entry in value_str:
        null_list = data[entry].isnull()  #The null case
        null_hwid = null_list[null_list==True].index.values
        if len(null_hwid):
            msg += "{} : None\n".format(entry.replace(":","."))
            msg += "num sats: {}\n".format(len(null_hwid))
            msg += textwrap.fill("      {}\n".format(', '.join(null_hwid)),50)

        # the comparison case 
        settings = set()
        for sat in data[entry].index.values:
            if sat not in null_hwid:
                settings.add(data.loc[sat][entry])
        for setting in settings:
            sat_w_set = data[entry][data[entry] == setting].index.values
            msg += "\n{} : {}\n".format(entry.replace(":","."),setting)
            msg += "num sats: {}\n".format(len(sat_w_set))
            msg += textwrap.fill("      {}\n".format(', '.join(sat_w_set)),50)
        msg += "\n\n"


    for entry in value_array:
        #finding all entries in array and finding what satellites have each of them
        array_elems = set()
        success = set()
        for sat in data[entry].index.values:
            if type(data.loc[sat][entry])!=float:
                for element in data.loc[sat][entry]:
                    array_elems.add(element)
        num_elements = len(array_elems)
        msg += "\nIn param {}, there are {} elements".format(entry.replace(":","."),
               num_elements)

        for array_elem in array_elems:
            for sat in data[entry].index.values:
                if type(data.loc[sat][entry])!=float:
                    if array_elem in data.loc[sat][entry]:
                        success.add(sat)
            msg += "\n\n\t{} : {} on these sats\n".format(entry.replace(":","."), array_elem)
            msg += "\tnum sats: {}\n".format(len(success))
            msg += textwrap.fill("      {}\n\n".format(', '.join(success)),50)

        msg += "\n"

    # find where the active payload does not match the desired payload
    for sat in data.index.values:
        active_payload_number = data.loc[sat]['active_payload']
        active_payload = data.loc[sat]['sc_payloads:{}:name'.format(active_payload_number)]
        desired_payload = data.loc[sat]['desired_payload']
        if desired_payload != active_payload:
            msg += "\nWARNING!! Sat {}'s active payload {}\n does not match its desired payload {}\n".format(
                   sat, active_payload, desired_payload)

    return msg


def main(): 
    data, num_sats = get_data()
    crit_num_sats = int(math.ceil(num_sats*MAJORITY_SATS))
    value_msg = value_analysis(data)
    gen_msg = count_keychains(data, crit_num_sats)
    report = value_msg + gen_msg

    return report 

if __name__ == "__main__":
    main()
