# Copyright 2016 PLanet Labs Inc.

import os
import glob
import sys
import tarfile
import shutil
import converters
import stat
import logging
from os.path import expanduser

# setup logger
log = logging.getLogger(__name__)
log_handler = logging.StreamHandler()
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
log_handler.setFormatter(formatter)
log.addHandler(log_handler)
log.setLevel(logging.INFO)

# directories of interest
BASE_TAR_DIR = expanduser('~') + '/analysis_report' + '/report_backups'
BASE_DASH_DIR = expanduser('~') + '/analysis_report' + '/dashboard'
BASE_AD_DIR = expanduser('~') + '/analysis_report' + '/change_frontend'

# get anomaly detection files
shutil.copyfile(BASE_AD_DIR+'/report.json', BASE_DASH_DIR+'/report.json')
shutil.copyfile(BASE_AD_DIR+'/alerts_report.html', BASE_DASH_DIR+'/alerts_report.html')

#find the newest flock report tar
newest_file = max(glob.iglob(BASE_TAR_DIR + '/*.tar.gz'), key=os.path.getctime)
log.info(newest_file)

#extracting the tar in the tar directory
tar = tarfile.open(newest_file)
tar.extractall(path=BASE_TAR_DIR)

#looping over files and formatting them, deleting duplicates
for member in tar.getmembers():
    log.info(member.name)

    if 'alerts_and_alarms' in member.name and member.name[-3:] == 'txt':
        shutil.move(BASE_TAR_DIR +'/'+ member.name, BASE_DASH_DIR +'/'+ member.name)
        converters.textToHTMLindividual(BASE_DASH_DIR, member.name)
        log.info('alerts attach found')
        os.remove(BASE_DASH_DIR + '/' + member.name)

    elif 'sat_stat' in member.name and member.name[-3:] == 'txt':
        shutil.move(BASE_TAR_DIR +'/'+ member.name, BASE_DASH_DIR +'/'+ member.name)
        converters.textToHTMLsat_stat(BASE_DASH_DIR, member.name)
        log.info('sat_stat found')
        os.remove(BASE_DASH_DIR + '/' + member.name)

    elif 'some-type-of-plots' in member.name and member.name[-3:] == 'pdf':
        shutil.move(BASE_TAR_DIR +'/'+ member.name, BASE_DASH_DIR +'/some-type-of-plots.pdf')
        log.info('some-type-of-plots found')

    elif 'some-other-type-of-plots' in member.name and member.name[-3:] == 'pdf':
        shutil.move(BASE_TAR_DIR +'/'+ member.name, BASE_DASH_DIR +'/some-other-type-of-plots.pdf')
        log.info('some-other-type-of-plots found')

    elif 'full-txt-report' in member.name and member.name[-3:] == 'txt':
        doc = ''
        with open(BASE_TAR_DIR +'/'+ member.name) as file:
            for line in file:
                doc += line
        end_of_body = doc.index('Satellites Ignored Globally:')
        body = doc[:end_of_body]
        body_file = open(BASE_DASH_DIR+'/fleet_report.txt', 'w+')
        body_file.write(body)
        body_file.close()
        converters.textToHTMLFleet(BASE_DASH_DIR)
        log.info('full txt report found')
        os.remove(BASE_TAR_DIR + '/' + member.name)
        os.remove(BASE_DASH_DIR + '/fleet_report.txt')

    else:
        os.remove(BASE_TAR_DIR + '/' + member.name)

tar.close()

# converting plots 
converters.plotsToHTML(BASE_DASH_DIR)


