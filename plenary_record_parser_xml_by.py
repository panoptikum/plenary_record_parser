# coding: utf-8
import os
import locale
import re
import logging
#import requests
import dataset
from lxml import html
#from urlparse import urljoin
import pandas as pd
import json
from datetime import datetime

os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')

from lib import helper

log = logging.getLogger(__name__)

locale.setlocale(locale.LC_TIME, "de_DE")

DATA_PATH = os.environ.get('DATA_PATH', '../data/BY')

# regular expressions to capture speeches of one session
BEGIN_STRING = r'^(?:<interjection_begin>)?\(Beginn:?\s+[0-9]{1,2}[.:][0-9]{1,2}\s+Uhr\)'
END_STRING = r'Schluss:?\s+[0-9]{1,2}[.:][0-9]{1,2}\s+Uhr'
CHAIR_STRING = r'^(Alterspräsident(?:in)?|Präsident(?:in)?|.{5,10}\s+Vizepräsident(?:in)?)\s+(.+?):'
SPEAKER_STRING = r'^(.+?)\s+\((SPD|CSU|FDP|FW|FREIE\s+WÄHLER|GRÜNE|fraktionslos)\):'
EXECUTIVE_STRING = r'^(Ministerpräsident(?:in)?|Staatsminister(?:in)?)\s+(.+?)<poi_end>'
OFFICIALS_STRING = r'^(Staatssekretär(?:in)?)\s+(.+?)\s+\((.+)'
EXT_GUEST_WP16_33_DAGMAR_STRING = r'^Landtagspräsidentin\s+a\.\s+D\.\s+Prof\.\s+Dr\.\s+Dagmar\s+Schi-'
EXT_GUEST_WP16_33_KUNZE_STRING = r'^Dr\.\s+h\.\s+c\.\s+Reiner\s+Kunze'

# compilation of regular expressions
# advantage combination of strings is possible
BEGIN_MARK = re.compile(BEGIN_STRING)
END_MARK = re.compile(END_STRING)
CHAIR_MARK = re.compile(CHAIR_STRING)
SPEAKER_MARK = re.compile(SPEAKER_STRING)
EXECUTIVE_MARK = re.compile(EXECUTIVE_STRING)
OFFICIALS_MARK = re.compile(OFFICIALS_STRING)
#SPEECH_ENDS = re.compile("|".join([CHAIR_STRING, SPEAKER_STRING, EXECUTIVE_STRING, OFFICIALS_STRING]))
INTERJECTION_MARK = re.compile(r'^<interjection_begin>\(')
INTERJECTION_END = re.compile(r'.+<interjection_end>$')
IDENTATION_MARK = re.compile(r'^<identation_begin>')
IDENTATION_END = re.compile(r'.+<identation_end>$')
POI_ONE_LINER = re.compile(r'(.+?)?<poi_end>')
HEADER_MARK = re.compile(r'^Bayerischer\s+Landtag\s+-\s+[0-9]{1,2}\.\s+Wahlperiode$|^Plenarprotokoll\s+[0-9]{1,2}/[0-9]{1,3}\s+v\.\s+[0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4}$')
HEADER_SPEAKER_MARK = re.compile(r'\((?:Abg\.)|\(Alterspräsident(?:in)?|\(Präsident(?:in)?|\(Vizepräsident(?:in)?|\(Staatssekretär(?:in)?|\(Minister(?:in)?|\(Justizminister(?:in)?:|\(Finanzminister(?:in):|\(Innenminister(?:in)?|\(Ministerpräsident(?:in)?')
EXT_GUEST_WP16_33_DAGMAR_MARK = re.compile(EXT_GUEST_WP16_33_DAGMAR_STRING)
EXT_GUEST_WP16_33_KUNZE_MARK = re.compile(EXT_GUEST_WP16_33_KUNZE_STRING)
NOT_AUTHORIZED = re.compile(r'^\([vV]om\s+Redner\s+nicht\s+autorisiert')
NO_INTERJECTION = re.compile(r'^.{2,5}\)')

LS_NO_INTERJECTION = ['(BayGVBl', '(Drs.']

DATE_CAPTURE = re.compile(r'([0-9]{1,2}\.[0-9]{2}\.[0-9]{4})')

STATE = 'BY'

files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith("xml.txt")]

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

ls_speeches = []
ls_interjection_length = []
ls_identation_length = []
ls_text_length = []

# debug mode
debug = True

for filename in files:
    #import pdb; pdb.set_trace()
    # extracts wp, session no. and if possible date of plenary session
    wp, session = str(int(filename[14:16])), str(int(filename[17:20]))
    #wp, sesion = 16, 1
    date = None

    print(str(wp), str(session))
       
    # deletes existing entries for this state's election period and session. e.g SH 18, 001
    table.delete(wp=wp, session=session, state=STATE)

    with open(filename, 'rb') as fh:
        text = fh.read().decode('utf-8')

    #import pdb; pdb.set_trace()
    print("Loading transcript: %s/%s, from %s" % (wp, session, filename))

    lines = text.replace('\xad', '-').split('\n')
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

    # trigger for identation blocks e.g. quotes
    identation = False
    identation_complete = None

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
        #if line=='Landtagspräsidentin  a.  D.  Prof.  Dr.  Dagmar  Schi-':
        #    import pdb; pdb.set_trace()
        # if line=='Franz Maget (SPD):   Frau Präsidentin, meine sehr ge-':
        #     import pdb; pdb.set_trace()
        if not date_captured and DATE_CAPTURE.search(line):
            date = DATE_CAPTURE.search(line).group(1)
            date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
            #import pdb; pdb.set_trace()
            print('date captured ' + date)
            date_captured = True
            continue
        elif not date_captured:
            continue
        if not in_session and BEGIN_MARK.search(line):
            in_session = True
            continue
        elif not in_session:
            continue
        #ignores header lines and page numbers
        if HEADER_MARK.match(line) or line.isdigit():
            continue

        if poi:
            if POI_ONE_LINER.match(line):
                if POI_ONE_LINER.match(line).group(1):
                    issue = issue + ' ' + POI_ONE_LINER.match(line).group(1)
                poi = False
                line = line.replace('<poi_end>', '')
            else:
                issue = issue + ' ' + line

        if '<poi_begin>' in line and not poi:
            line = line.replace('<poi_begin>', '')
            if CHAIR_MARK.match(line):
            #import pdb; pdb.set_trace()
                s = CHAIR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2))
                president = True
                executive = False
                servant = False
                party = None
                role = s.group(1)
            elif EXECUTIVE_MARK.match(line):
                s = EXECUTIVE_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2))
                role = 'executive'
                party = None
                president = False
                executive = True
                servant = False
            elif OFFICIALS_MARK.match(line):
                s = OFFICIALS_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2))
                party = None
                president = False
                executive = False
                servant = True
                role = s.group(1)
            elif SPEAKER_MARK.match(line):
                s = SPEAKER_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = s.group(2)
                role = 'mp'
            elif wp=='16' and session=='33':
                if EXT_GUEST_WP16_33_DAGMAR_MARK.match(line):
                    s = EXT_GUEST_WP16_33_DAGMAR_MARK.match(line)
                    new_speaker = 'Schipanski'
                    president = False
                    executive = False
                    servant = False
                    party = None
                    role = 'guest'
                elif EXT_GUEST_WP16_33_KUNZE_MARK.match(line):
                    s = EXT_GUEST_WP16_33_KUNZE_MARK.match(line)
                    new_speaker = 'Kunze'
                    president = False
                    executive = False
                    servant = False
                    party = None
                    role = 'guest'
            else:
                #import pdb; pdb.set_trace()
                if POI_ONE_LINER.match(line):
                    issue = POI_ONE_LINER.match(line).group(1)
                else:
                    issue = line
                    poi = True

            line = line.replace('<poi_end>', '')

        if not poi and current_speaker:
            new_speaker = new_speaker.replace('<poi_end>', '')
        # if new_speaker=='nung ist jetzt möglich! - Adelheid Rupp':
        #     import pdb; pdb.set_trace()
        if s is not None and current_speaker is not None:
            #import pdb; pdb.set_trace()
            if new_speaker!=current_speaker or END_MARK.search(line) or not has_more:
                # concatenates lines to one string
                #text = [i + ' ' if not i.endswith('-') else i for i in text]
                text_length = len(text)
                text = ''.join(text)
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                # removes whitespaces at the beginning and end
                text = text.strip()
                text = re.sub('-(?=[a-z])', '', text)
                text = re.sub(r'\([vV]om\s+[Rr]edner\s+nicht\s+autorisiert\)', '', text)
                if text:
                    speech = pd.DataFrame({'speaker': [current_speaker], 
                                           'party': [current_party], 
                                           'speech': [text], 
                                           'seq': [seq],
                                           'sub': [sub],
                                           'exec': current_executive,
                                           'servant': current_servant,
                                           'wp': wp,
                                           'session': session,
                                           'president': current_president,
                                           'role': [current_role],
                                           'state': [STATE],
                                           'interjection': interjection,
                                           'identation': identation,
                                           'date': [date],
                                           'issue': issue})
                    speeches.append(speech)

                    ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])

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
                    print('matched end mark')
                    break
                seq += 1
                sub = 0
                current_speaker = None
        # adds interjections to the data in such a way that order is maintained
        if INTERJECTION_MARK.match(line.lstrip()) and not interjection:
            line = line.replace('<interjection_begin>', '')
            # skips lines that simply refer to the current speaker
            # if HEADER_SPEAKER_MARK.match(line):
            #     continue
            #text = [i + ' ' if not i.endswith('-') else i for i in text]
             # concatenates lines to one string
            interjection_length = 0 
            if not interjection_complete and current_speaker is not None:
                #import pdb; pdb.set_trace()
                text_length = len(text)
                text = [i for i in text]
                text = ''.join(text)
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                # removes whitespaces at the beginning and end
                text = text.strip()
                text = re.sub('-(?=[a-z])', '', text)
                text = re.sub(r'\([vV]om\s+[Rr]edner\s+nicht\s+autorisiert\)', '', text)
                if text:
                    speech = pd.DataFrame({'speaker': [current_speaker], 
                                           'party': [current_party], 
                                           'speech': [text], 
                                           'seq': [seq],
                                           'sub': [sub],
                                           'exec': current_executive,
                                           'servant': current_servant,
                                           'wp': wp,
                                           'session': session,
                                           'president': current_president,
                                           'role': [current_role],
                                           'state': [STATE],
                                           'interjection': interjection,
                                           'identation': identation,
                                           'date': date,
                                           'issue': issue})
                    speeches.append(speech)
                    ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])

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
            #import pdb; pdb.set_trace()
            sub += 1
            interjection = True
            interjection_text = []
        if interjection:
            #import pdb; pdb.set_trace()
            if INTERJECTION_END.match(line.rstrip()):
                line = line.replace('<interjection_end>', '')
                if current_speaker is not None:
                    interjection_text.append(line)
                    interjection_text = [i + ' ' if not i.endswith('-') else i for i in interjection_text]
                    interjection_text = ''.join(interjection_text)
                    # removes whitespace duplicates
                    interjection_text = re.sub(' +', ' ', interjection_text)
                    # removes whitespaces at the beginning and end
                    interjection_text = interjection_text.strip()
                    interjection_text = re.sub('-(?=[a-z])', '', interjection_text)
                    if interjection_text:
                        speech = pd.DataFrame({'speaker': [current_speaker], 
                                           'party': [current_party], 
                                           'speech': [interjection_text], 
                                           'seq': [seq],
                                           'sub': [sub],
                                           'exec': current_executive,
                                           'servant': current_servant,
                                           'wp': wp,
                                           'session': session,
                                           'president': current_president,
                                           'role': [current_role],
                                           'state': [STATE],
                                           'interjection': [interjection],
                                           'identation': identation,
                                           'date': date,
                                           'issue': issue})
                        speeches.append(speech)
                        ls_interjection_length.append([interjection_length, wp, session, seq, sub, current_speaker, interjection_text])

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
                interjection = False
                interjection_complete = True
                interjection_skip = False
                continue
            else:
                interjection_text.append(line)
                interjection_length += 1
                continue
        if IDENTATION_MARK.match(line.lstrip()) and not identation:
            #import pdb; pdb.set_trace()
            line = line.replace('<identation_begin>', '')
            identation_length = 0
            if not identation_complete and current_speaker is not None:
                text_length = len(text)
                text = [i for i in text]
                text = ''.join(text)
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                # removes whitespaces at the beginning and end
                text = text.strip()
                text = re.sub('-(?=[a-z])', '', text)
                text = re.sub(r'\([vV]om\s+[Rr]edner\s+nicht\s+autorisiert\)', '', text)
                if text:
                    speech = pd.DataFrame({'speaker': [current_speaker], 
                                           'party': [current_party], 
                                           'speech': [text], 
                                           'seq': [seq],
                                           'sub': [sub],
                                           'exec': current_executive,
                                           'servant': current_servant,
                                           'wp': wp,
                                           'session': session,
                                           'president': current_president,
                                           'role': [current_role],
                                           'state': [STATE],
                                           'interjection': interjection,
                                           'identation': identation,
                                           'date': date,
                                           'issue': issue})
                    speeches.append(speech)
                    ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])

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
                                       'date': date}

                    table.insert(speech_dict)
            #import pdb; pdb.set_trace()
            sub += 1
            identation = True
            identation_text = []
        if identation:
            #import pdb; pdb.set_trace()
            if IDENTATION_END.match(line.rstrip()):
                line = line.replace('<identation_end>', '')
                if current_speaker is not None:
                    identation_text.append(line)
                    identation_text = [i + ' ' if not i.endswith('-') else i for i in identation_text]
                    identation_text = ''.join(identation_text)
                    # removes whitespace duplicates
                    identation_text = re.sub(' +', ' ', identation_text)
                    # removes whitespaces at the beginning and end
                    identation_text = identation_text.strip()
                    identation_text = re.sub('-(?=[a-z])', '', identation_text)
                    if identation_text.rstrip().endswith(')') and speech.loc[0, 'interjection']:
                        #import pdb; pdb.set_trace()
                        identation = False
                        interjection = True
                        idx_last_speech = len(speeches)-1
                        speeches[idx_last_speech].loc[0, 'speech'] = speeches[idx_last_speech].loc[0, 'speech'] + ' ' + identation_text
                    elif identation_text:
                        speech = pd.DataFrame({'speaker': [current_speaker], 
                                           'party': [current_party], 
                                           'speech': [identation_text], 
                                           'seq': [seq],
                                           'sub': [sub],
                                           'exec': current_executive,
                                           'servant': current_servant,
                                           'wp': wp,
                                           'session': session,
                                           'president': current_president,
                                           'role': [current_role],
                                           'state': [STATE],
                                           'interjection': [interjection],
                                           'identation': [identation],
                                           'date': date,
                                           'issue': issue})
                        speeches.append(speech)
                        ls_identation_length.append([identation_length, wp, session, seq, sub, current_speaker, identation_text])

                        speech_dict = {'speaker': current_speaker,
                                       'party': current_party,
                                       'speech': identation_text,
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
                identation = False
                interjection = False # if identation is assigned 
                identation_complete = True
                identation_skip = False
                continue
            else:
                identation_text.append(line)
                identation_length += 1
                continue
        if current_speaker is not None:
            if interjection_complete or identation_complete:
                interjection_complete = None
                identation_complete = None
                text = []
                line = helper.cleans_line_by(line)
                if line:
                    text.append(line)
                continue
            else:
            #if debug:
            #    import pdb; pdb.set_trace()
                # if dotNotOnFirstLine:
                #     if ":* " in line:
                #         parts = line.split(':* ', 1)
                #         line = parts[-1]
                #         current_role = ''.join([current_role, parts[0]]).replace(')', '')
                #         dotNotOnFirstLine = False
                #     elif ":" in line:
                #         parts = line.split(':', 1)
                #         line = parts[-1]
                #         current_role = ''.join([current_role, parts[0]]).replace(')', '')
                #         dotNotOnFirstLine = False
                #     elif line.isspace():
                #         current_role = current_role.replace(')', '')
                #         dotNotOnFirstLine = False
                #     else:
                #         current_role = ''.join([current_role, line])
                #         line = ''
                current_role = current_role.strip()
                line = helper.cleans_line_by(line)
                if line:
                    text.append(line)
                continue

        if s is not None:
            #if debug:
            #    import pdb; pdb.set_trace()
            #role = s.group(1)
            # dotNotOnFirstLine = False
            if ":* " in line:
                line = line.split(':* ', 1)[-1]
            elif ":" in line:
                line = line.split(':', 1)[-1]
            line = helper.cleans_line_by(line)
            # else:
            #     line = ''
            #     dotNotOnFirstLine = True
            text = []
            if line:
                text.append(line)
            current_speaker = new_speaker
            current_party = party
            current_president = president
            current_executive = executive
            current_servant = servant
            current_role = role

        if not has_more and (in_session or interjection):
            print(str(wp) + ' ' + str(session) + ' : no match for end mark -> error')

    pd_session_speeches = pd.concat(speeches)
    ls_speeches.append(pd_session_speeches)

pd_speeches = pd.concat(ls_speeches).reset_index()
pd_speeches.to_csv(os.path.join(DATA_PATH, STATE + '_test.csv'))

# checks
# interjection length
if debug:
    idx = [i for i, e in enumerate(ls_interjection_length) if e[0] >= 10]

    #text length
    idx_txt = [i for i, e in enumerate(ls_text_length[0:10]) if e[0] > 30]

    pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()