# Copyright 2016 PLanet Labs Inc.

from Package import UpstreamPackage
from datetime import timedelta
import time
import calendar
import numpy as np
import pandas as pd
import requests
import json
import datetime

TOKEN = 'insert_token_here'
HEADERS = {'Authorization': 'Token {}'.format(TOKEN)}

ON_CURRENT = 100
no_ADM_sats = ['some', 'satellites']


def get_data(sat, rep):
    # takes data from taaser

    up = UpstreamPackage()

    # get start and end times in ms
    alarm_hours = round(rep.pars.span_hours, 2)
    start_time = rep.da.alarms_end_time - timedelta(hours=alarm_hours)
    start_time_unix_ms = calendar.timegm(start_time.utctimetuple())*1000
    end_time_unix_ms = calendar.timegm(rep.da.alarms_end_time.utctimetuple())*1000

    channels_of_interest = ['batt-temp-sensor-1', 'batt-temp-sensor-2',
                            'batt-temp-sensor-3', 'batt-temp-sensor-4',
                            'batt-heat-current-1',
                            'batt-heat-current-2',
                            'batt-heat-current-3',
                            'batt-heat-current-4',
                            'batt-heat-current-vc1',
                            'batt-heat-current-vc2']
 
    sat_df = up.get_data(sat, channels_of_interest,
                                           start_time_unix_ms,
                                           end_time_unix_ms)

    return sat_df


def temp_data(data):
    # takes data and extracts the batt temp channels
    temps = data[['batt-temp-sensor-1', 'batt-temp-sensor-2', 'batt-temp-sensor-3',
                     'batt-temp-sensor-4']][(data['att-temp-sensor-1'] >= -270)]
    irid_temps =  data[['batt-temp-sensor-3', 'batt-temp-sensor-4']][(data['batt-temp-sensor-3'] >= -270)]
    cpu_temps  =  data[['batt-temp-sensor-1', 'batt-temp-sensor-2']][(data['batt-temp-sensor-1'] >= -270)]

    return cpu_temps, irid_temps, temps


def heater_current_data(self, data, build):
    # takes data and extracts the heater current channels
    if build < 13:
        cpu_currents = data[['batt-heat-current-1',
                 'batt-heat-current-2']][(data['batt-heat-current-1'] > 0.1)]
        irid_currents = data[['batt-heat-current-3',
                 'batt-heat-current-4']][(data['batt-heat-current-3'] > 0.1)]
        currents = data[['batt-heat-current-1',
                 'batt-heat-current-2', 'batt-heat-current-3',
                 'batt-heat-current-4']][(data['batt-heat-current-1'] > 0.1)]
        return cpu_currents, irid_currents, currents

    if build >= 13:
        side1_currents = data[['batt-heat-current-vc1']][(data['batt-heat-current-vc1'] > 0.1)]
        side2_currents = data[['batt-heat-current-vc2']][(data['batt-heat-current-vc2'] > 0.1)]
        currents = data[['batt-heat-current-vc1',
                         'batt-heat-current-vc2']][(data['batt-heat-current-vc1'] > 0.1)]
        return side1_currents, side2_currents, currents


def get_thresholds(sat):
    # sends query to MC API for the state metadata to find batt heater thresholds and build

    URL = 'https://planet-labs-url.com/api/url/satellites/{}/'.format(sat)
    task = requests.get(URL, headers=HEADERS)
    dictionary = task.json()
    dictionary_state = dictionary['state_data']
    json_state = json.loads(dictionary_state)
    defaults = json_state['default_params']
    build = dictionary['hw_build']

    thresholds = []
    sat_build = 0

    for elem in defaults:
        if 'thermostat' in elem:
            thermo_list = elem.split(' ')
            thresholds = [int(f) for f in thermo_list if f.isdigit()]
            thresholds.sort()
            break

    if build:
        sat_build = int(build)

    if not thresholds:
        if sat_build == 9:
            return None, 9
        elif sat_build >= 13:
            return [12, 18], 13

    return thresholds, sat_build


def no_batt_heaters(temps):
    if len(temps[temps.values < 5]) > 0:
        msg = "\t{} {}\n".format("(no heaters) batt.temp.bus min:", temps.values.min())
        return msg
    else: 
        return ''


def inactive_heaters(temps):
    if len(temps[temps.values < 5]) > 0:
        msg = "\t{} {}\n".format("(heaters not on) batt.temp.bus min:", temps.values.min())
        return msg
    else:
        return ''


def no_ADM(temps):
    if len(temps[temps.values < 5]) > 0:
        msg = "\t{} {}\n".format("(No ADM) batt.temp.bus min:", temps.values.min())
        return msg
    else:
        return ''


def cold_batt_check(cpu_T, cpu_I, irid_T, irid_I, thresholds, self, sat, temps, currents, build):
    msg = ''
    error = ''

    t_min = thresholds[0]
    on_bool = False
    off_cpu_bool = False
    off_irid_bool = False

    for col in temps:
    # for each sensor on sat
        old_elem = t_min
        counter = 0
        for elem in list(temps[col].values):
        # for each value in sensor
            if (np.sign(elem-t_min) != np.sign(old_elem-t_min)) and (np.sign(elem-t_min) == -1):
            # if goes below temp min setpoint
                start_cold_time = temps[col].index[counter]

                temp_filter_df = temps[[col]][temps[col] >= t_min]
                time_filter_df = temp_filter_df[temp_filter_df.index >=
                                                       start_cold_time]
                # find start and end times of cold cycle
                if len(time_filter_df.index.values) > 0:
                    time_filter_df.sort_index()
                    end_cold_time = time_filter_df.index.values[0]
                else:
                    old_elem = elem
                    counter += 1
                    continue

                # find current and temp data during cold cycle
                cold_currents = currents[(currents.index.values >= start_cold_time) &
                                            (currents.index.values <= end_cold_time)]
                cold_temps = temps[(temps.index.values >= start_cold_time) &
                                       (temps.index.values <= end_cold_time)]

                # is there enough data to make an alert about
                if (len(cold_temps.index) > 4) and (len(cold_currents.index) > 5):
                    min_temp = cold_temps.values.min()  # min temp in cycle
                    #min_temp = temps.values.min()  # absolute minimum temp
                else:
                    old_elem = elem
                    counter += 1
                    continue

                # see if specific side's things come on ever and alert appropriately
                if col in cpu_T.columns.values:
                    if build < 13:
                        cpu_cold_I = cold_currents[['batt-heat-current-1',
                                                   'batt-heat-current-2']]
                        side = 'cpu'
                    else:
                        cpu_cold_I = cold_currents[['batt-heat-current-vc1']]
                        side = 'vc1'

                    if cpu_cold_I.values.any():
                        max_current = cpu_cold_I.values.max()
                    else:
                        old_elem = elem
                        counter += 1
                        continue

                    if max_current > ON_CURRENT and not on_bool and min_temp < -5:
                        error = "Battery was colder than {}C and heaters turned".format(t_min)
                        error += " on, min temp: {:.1f}C".format(min_temp)
                        self.da.log.info('{} {} side cold {:.2f} and heaters on {:.2f} at {}-{}'.format(
                                         sat, side, elem, max_current, start_cold_time, end_cold_time))
                        on_bool = True
                    if max_current < ON_CURRENT and not off_cpu_bool:
                        error = 'Battery was colder than {}C, heaters did not'.format(t_min)
                        error += ' turn on ({}), min temp: {:.1f}C'.format(side, min_temp)
                        self.da.log.info('{} {} side cold {:.2f} and heaters not on {:.2f} at {}-{}'.format(
                                             sat, side, elem, max_current, start_cold_time, end_cold_time))
                        off_cpu_bool = True
                            
                elif col in irid_T.columns.values:
                    if build < 13:
                        irid_cold_I = cold_currents[['batt-heat-current-3',
                                                   'batt-heat-current-4']]
                        side = 'irid'
                    else:
                        irid_cold_I = cold_currents[['batt-heat-current-vc2']]
                        side = 'vc2'

                    if irid_cold_I.values.any():
                        max_current = irid_cold_I.values.max()
                    else:
                        old_elem = elem
                        counter += 1
                        continue

                    if max_current > ON_CURRENT and not on_bool and min_temp < -5:
                        error = "Battery was colder than {}C and heaters turned".format(t_min)
                        error += " on, min temp: {:.1f}C".format(min_temp)
                        self.da.log.info('{} {} side cold {:.2f} and heaters on {:.2f} at {}-{}'.format(
                                          sat, side, elem, max_current, start_cold_time, end_cold_time))
                        on_bool = True
                    if max_current < ON_CURRENT and not off_irid_bool:
                        error = 'Battery was colder than {}C, heaters did not'.format(t_min)
                        error += ' turn on ({}), min temp: {:.1f}C'.format(side, min_temp)
                        self.da.log.info('{} {} side cold {:.2f} and heaters not on {:.2f} at {}-{}'.format(
                                              sat, side, elem, max_current, start_cold_time, end_cold_time))
                        off_irid_bool = True

                else:
                    self.da.log.info('Data columns are not as expected in battery heater')
                    old_elem = elem
                    counter += 1
                    continue

            # reset loop
            old_elem = elem
            if error:
                msg += "\t" + error + "\n"
            error = ''
            counter += 1

            if on_bool and off_cpu_bool and off_irid_bool:
               # stop looping when all possible errors attained
               return msg

    return msg


def sat_alerts(self, sat):

    data = get_data(sat, self)
    thresholds, build = get_thresholds(sat)
    cpu_1_temps, irid_2_temps, temps = temp_data(data)

    # case of no battery heaters (09XX)
    if not thresholds:
        messages = no_batt_heaters(temps)
        return messages

    # for build 13's
    if build >= 13:
        side1_I, side2_I, currents = heater_current_data(self, data, build)
        if len(currents[currents.values > 100]) > 0:
            messages = cold_batt_check(cpu_1_temps, side1_I, irid_2_temps, side2_I, thresholds[:-1],
                                                                  self, sat, temps, currents, build)
        elif len(currents[currents.values > 0]) <= 0 and sat in no_ADM_sats:
            messages = no_ADM(temps)
        else:
            messages = inactive_heaters(temps)
        return messages

    cpu_currents, irid_currents, currents = heater_current_data(self, data, build)
    messages = cold_batt_check(cpu_1_temps, cpu_currents, irid_2_temps, irid_currents, thresholds,
                                                                self, sat, temps, currents, build)
    
    return messages

