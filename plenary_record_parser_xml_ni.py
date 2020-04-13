# coding: utf-8
import os
import locale
import re
import logging
# import requests
import dataset
from lxml import html
# from urlparse import urljoin
import pandas as pd
from datetime import datetime

os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')

from lib import helper

log = logging.getLogger(__name__)

locale.setlocale(locale.LC_TIME, "de_DE.utf-8")

STATE = 'NI'

DATA_PATH = os.environ.get('DATA_PATH', '../data/' + STATE)

BEGIN_MARK = re.compile(r'^(?:Beginn der Sitzung|Beginn):\s(?:[0-9]{1}|[0-9]{2})(?:.[0-9]{2})?\sUhr')
END_MARK = re.compile(r'^(?:Schluss\s+der\s+Sitzung|Schluss\s+der\s+Sitzung):\s+(?:[0-9]{2}|[0-9]{1}).[0-9]{2}(?:\s+Uhr)?')
CHAIR_MARK = re.compile(r'^<poi_begin>(Präsident(?:in)?|Alterspräsident(?:in)?|Vizepräsident(?:in)?)\s(.+?)<poi_end>')
SPEAKER_MARK = re.compile(r'^<poi_begin>(.+)(?:(?:<poi_end>\s+|\s+<poi_end>)\((SPD|CDU|FDP|GRÜNE|AfD)\)|\s+\((.+)\):?(?:\s+)?<poi_end>)')
EXECUTIVE_MARK = re.compile(r'^<poi_begin>(.+?)<poi_end>,\s+(?:[Mm]inister(?:in)?|Ministerpräsident|Finanzminister(?:in)?|Kultusminister(?:in)?|Justizminister(?:in)?)')
OFFICIALS_MARK = re.compile(r'^(Staatssekretärin|Staatssekretär)\s(.*?)\s?([A-Za-zßÜÖÄäöü\-]+)?:')
INTERJECTION_MARK = re.compile(r'^<interjection_begin>(?:<poi_begin>)?\(')
INTERJECTION_END = re.compile(r'.+\)<interjection_end>$')
#TOP_MARK = re.compile('.*(rufe.*die Frage|zur Frage|der Tagesordnung|Tagesordnungspunkt|Zusatzpunkt).*')
#POI_MARK = re.compile('\((.*)\)\s*$', re.M)
#HEADER_MARK = re.compile(r'(^\bSitzung\b.*?\b[0-9]{4}\b$)|([0-9]{4}.*?(Bremische Bürgerschaft))|(\(Landtag\).*?(Wahlperiode).*?(Sitzung))|(Bremische Bürgerschaft \(Landtag\).*?(Wahlperiode))')
#PAGENUMBER_MARK = re.compile(r'[0-9]{4}\s')
DATE_CAPTURE = re.compile(r'Hannover,\s+den\s+([0-9]{1,2}\.\s+.+?\s+[0-9]{4})')
POI_ONE_LINER = re.compile(r'(.+?)?<poi_end>(?:.+)?')

files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith("xml.txt")]

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

ls_speeches = []
ls_interjection_length = []
ls_text_length = []
ls_interjection_length = []

for filename in files:

    wp, session = int(filename[14:16]), int(filename[23:26])
    print(wp, session)

    with open(filename, 'rb') as fh:
        text = fh.read().decode('utf-8')

    print("Loading transcript: %s/%.3d, from %s" % (wp, session, filename))

    lines = text.split('\n')

    # trigger to skip lines until date is captured
    date_captured = False
    # trigger to skip lines until in_session mark is matched
    in_session = False

    # poi
    poi = False
    issue = None
    concat_issues = False


    # variable captures contain new speaker if new speaker is detected
    new_speaker = None
    # contains current speaker, to use actual speaker and not speaker that interrupts speech
    current_speaker = None
    s = None

    # trigger to check whether a interjection is found
    interjection = False
    interjection_complete = None
    cnt_brackets_opening = 0
    cnt_brackets_closing = 0
    missing_closing = False

    # identation
    identation = False

    # trigger to find parts where zwischenfragen are continued without labelling current speaker
    zwischenfrage = False
    speaker_cnt = 0

    # dummy variables and categorial variables to characterize speaker
    president = False
    executive = False
    servant = False
    party = None
    role = None

    # counts to keep order
    seq = 0
    sub = 0

    # contains list of dataframes, one df = one speech
    speeches = []

    for line, has_more in helper.lookahead(lines):
        if not date_captured and DATE_CAPTURE.search(line):
            date = DATE_CAPTURE.search(line).group(1)
            date = datetime.strptime(date, '%d. %B %Y').strftime('%Y-%m-%d')
            print('date captured ' +  date)
            date_captured = True
            continue
        elif not date_captured:
            continue
        if not in_session and BEGIN_MARK.search(line):
            in_session = True
            continue
        elif not in_session:
            continue

        if line.replace('<interjection_begin>', '').replace('<interjection_end>', '').strip().isdigit():
            continue

        if poi:
            if POI_ONE_LINER.match(line):
                if POI_ONE_LINER.match(line).group(1):
                    issue = issue + ' ' + POI_ONE_LINER.match(line).group(1)
                issue = issue.replace('<poi_begin>', '')
                issue = issue.replace('<poi_end>', '')
                poi = False
                line = line.replace('<poi_end>', '')
            else:
                issue = issue + ' ' + line
        if '<poi_begin>' in line:   
            if CHAIR_MARK.match(line):
                s = CHAIR_MARK.match(line)
                new_speaker = s.group(2).replace(':', '').strip()
                president = True
                executive = False
                servant = False
                party = None
                role = 'chair'
            elif SPEAKER_MARK.match(line):
                s = SPEAKER_MARK.match(line)
                new_speaker = s.group(1)
                president = False
                executive = False
                servant = False
                party = s.group(2)
                role = 'mp'
            elif EXECUTIVE_MARK.match(line):
                s = EXECUTIVE_MARK.match(line)
                new_speaker = s.group(1)
                role = 'executive'
                party = None
                president = False
                executive = True
                servant = False
            # elif OFFICIALS_MARK.match(line):
            #     s = OFFICIALS_MARK.match(line)
            #     new_speaker = s.group(2)
            #     party = None
            #     president = False
            #     executive = False
            #     servant = True
            #     role = s.group(1)
            else:
                if POI_ONE_LINER.match(line):
                    issue = POI_ONE_LINER.match(line).group(1)
                    issue = issue.replace('<poi_begin>', '')
                    issue = issue.replace('<poi_end>', '')
                else:
                    issue = line
                    poi = True

            line = line.replace('<poi_begin>', '').replace('<poi_end>', '')
        if s is not None and current_speaker is not None:
            if new_speaker!=current_speaker or END_MARK.search(line) or not has_more:
                # concatenates lines to one string
                #text = [i + ' ' if not i.endswith('-') else i for i in text]
                text = ''.join(text).replace('<poi_end>', '')
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                text = text.replace('<interjection_begin>', '')
                text = text.replace('<interjection_end>', '')
                # removes whitespaces at the beginning and end
                text = text.strip()
                text = re.sub('-(?=[a-z])', '', text)
                if text:
                    speech = pd.DataFrame({'speaker': [current_speaker], 
                                           'party': [current_party], 
                                           'speech': [text], 
                                           'seq': [seq],
                                           'sub': [sub],
                                           'executive': current_executive,
                                           'servant': current_servant,
                                           'wp': wp,
                                           'session': session,
                                           'president': current_president,
                                           'role': [current_role],
                                           'state': [STATE],
                                           'interjection': interjection,
                                           'date': [date],
                                           'issue': issue})
                    speeches.append(speech)
                    speech_dict = {'speaker': current_speaker,
                                       'party': current_party,
                                       'speech': text,
                                       'seq': seq,
                                       'sub': sub,
                                       'executive': current_executive,
                                       'servant': current_servant,
                                       'wp': wp,
                                       'session': session,
                                       'president': current_president,
                                       'role': current_role,
                                       'state': STATE,
                                       'interjection': interjection,
                                       'identation': identation,
                                       'date': date,
                                       'issue': issue}

                    table.insert(speech_dict)

                if END_MARK.search(line):
                    in_session = False
                    break
                seq += 1
                sub = 0
                current_speaker = None

        if INTERJECTION_MARK.match(line):
            line = line.replace('<interjection_begin>', '')
            interjection_length = 0
            if not interjection_complete and current_speaker is not None:
                text = ''.join(text).replace('<poi_end>', '')
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                text = text.replace('<interjection_begin>', '')
                text = text.replace('<interjection_end>', '')
                # removes whitespaces at the beginning and end
                text = text.strip()
                text = re.sub('-(?=[a-z])', '', text)
                speech = pd.DataFrame({'speaker': [current_speaker], 
                                       'party': [current_party], 
                                       'speech': [text], 
                                       'seq': [seq],
                                       'sub': [sub],
                                       'executive': current_executive,
                                       'servant': current_servant,
                                       'wp': wp,
                                       'session': session,
                                       'president': current_president,
                                       'role': [current_role],
                                       'state': [STATE],
                                       'interjection': interjection,
                                       'date': date,
                                       'issue': issue})
                speeches.append(speech)
                speech_dict = {'speaker': current_speaker,
                                       'party': current_party,
                                       'speech': text,
                                       'seq': seq,
                                       'sub': sub,
                                       'executive': current_executive,
                                       'servant': current_servant,
                                       'wp': wp,
                                       'session': session,
                                       'president': current_president,
                                       'role': current_role,
                                       'state': STATE,
                                       'interjection': interjection,
                                       'identation': identation,
                                       'date': date,
                                       'issue': issue}

                table.insert(speech_dict)

            sub += 1
            interjection = True
            interjection_text = []
        if interjection:
            if INTERJECTION_END.match(line):
                line = line.replace('<interjection_end>', '')
                if current_speaker is not None:
                    if line and not line.isspace():
                        interjection_text.append(line)
                    interjection_text = [i + ' ' if not i.rstrip('<interjection_end>').endswith('-') else i.rstrip('-') for i in interjection_text]
                    interjection_text = ''.join(interjection_text)
                    # removes whitespace duplicates
                    interjection_text = re.sub(' +', ' ', interjection_text)
                    interjection_text = interjection_text.replace('<interjection_begin>', '')
                    interjection_text = interjection_text.replace('<interjection_end>', '')
                    # removes whitespaces at the beginning and end
                    interjection_text = interjection_text.strip()
                    interjection_text = re.sub('-(?=[a-z])', '', interjection_text)
                    speech = pd.DataFrame({'speaker': [current_speaker], 
                                       'party': [current_party], 
                                       'speech': [interjection_text], 
                                       'seq': [seq],
                                       'sub': [sub],
                                       'executive': current_executive,
                                       'servant': current_servant,
                                       'wp': wp,
                                       'session': session,
                                       'president': current_president,
                                       'role': [current_role],
                                       'state': [STATE],
                                       'interjection': [interjection],
                                       'date': date,
                                       'issue': issue})
                    speeches.append(speech)

                    speech_dict = {'speaker': current_speaker,
                                       'party': current_party,
                                       'speech': interjection_text,
                                       'seq': seq,
                                       'sub': sub,
                                       'executive': current_executive,
                                       'servant': current_servant,
                                       'wp': wp,
                                       'session': session,
                                       'president': current_president,
                                       'role': current_role,
                                       'state': STATE,
                                       'interjection': interjection,
                                       'identation': identation,
                                       'date': date,
                                       'issue': issue}

                    table.insert(speech_dict)


                    sub += 1
                    interjection_length += 1
                    ls_interjection_length.append([interjection_length, wp, session, seq, sub, current_speaker, interjection_text])
                interjection = False
                interjection_complete = True
                interjection_skip = False
                cnt_brackets_opening = 0
                cnt_brackets_closing = 0
                continue
            else:
                if line and not line.isspace():
                    interjection_text.append(line)
                interjection_length += 1
                continue

        if current_speaker is not None:
            if interjection_complete:
                interjection_complete = None
                text = []
                line = helper.cleans_line(line)
                if line and not line.isspace():
                    text.append(line)
                continue
            else:
                current_role = current_role.strip()
                line = helper.cleans_line(line)
                if line and not line.isspace():
                    text.append(line)
                continue

        if s is not None:
            if ":* " in line:
                line = line.split(':* ', 1)[-1]
            elif ":" in line:
                line = line.split(':', 1)[-1]
            line = helper.cleans_line(line)
            text = []
            if line and not line.isspace():
                text.append(line)
            current_speaker = new_speaker
            current_party = party
            current_president = president
            current_executive = executive
            current_servant = servant
            current_role = role
        if not has_more and in_session:
            print(str(wp) + ' ' + str(session) + ' : no match for end mark -> error')

    pd_session_speeches = pd.concat(speeches)
    print(str(pd_session_speeches.seq.max()) + ' speeches detected and ' + str(pd_session_speeches.loc[pd_session_speeches.interjection==True].interjection.count()) + ' interjections')
    ls_speeches.append(pd_session_speeches)

pd_speeches = pd.concat(ls_speeches).reset_index()
pd_speeches.to_csv(os.path.join(DATA_PATH, STATE + '_test.csv'))



# checks
# interjection length
idx = [i for i, e in enumerate(ls_interjection_length) if e[0] > 10]

#text length
idx_txt = [i for i, e in enumerate(ls_text_length[0:10]) if e[0] > 15]

pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()