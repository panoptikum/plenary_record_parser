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
# os.chdir('/home/felix/privat/plenarprotokolle/code')

from lib import helper

log = logging.getLogger(__name__)

locale.setlocale(locale.LC_TIME, "de_DE.utf-8")

DATA_PATH = os.environ.get('DATA_PATH', '../data/BE')

BEGIN_MARK = re.compile(r'eröffnet\s+die\s+Sitzung|eröffnet\s+die|Alterspräsidentin\s+Bruni\s+Wildenhein-Lauterbach\s+eröffnet ')
END_MARK = re.compile(r'(?:^\[Schluss der Sitzung|Schluss der Sitzung:\s+[0-9]{1,2}\.[0-9]{1,2}\s+Uhr\])')
CHAIR_MARK = re.compile(r'^(Präsident(?:in)?|Vizepräsident(?:in)?|Alterspräsident(?:in)?)\s(.*?)(\s\(.*?\))?:')
SPEAKER_MARK = re.compile(r'^(.+?)\s+\((SPD|CDU|LINKE|GRÜNE|AfD|FDP|fraktionslos|PIRATEN|Grüne|(?:.+?\s+)?Berichterstatter(?:in)?)\)')
EXECUTIVE_MARK = re.compile(r'^(Regierender Bürgermeister|Bürgermeister(?:in)?|Senator(?:in)?)\s(.*)')
OFFICIALS_MARK = re.compile(r'^(Staatssekretär(?:in)?)\s(.*?)\s\(')
DATA_PROTECTION_MARK = re.compile(r'(.+?)\s+\((?:Berliner\s+Beauftragter?\s+für\s+Datenschutz|Datenschutzbeauftragter?)')
LHR_MARK = re.compile(r'(.+?)\s+<poi_end>\(Präsident(?:in)?\s+des\s+Rechnungshofes')
INTERJECTION_MARK = re.compile(r'^\[')
INTERJECTION_END = re.compile(r'.+?\]$')
POI_ONE_LINER = re.compile(r'(.+?)?<poi_end>(?:.+)?')
WHITE_SPACE_ONLY = re.compile(r'^\s+$')
#TOP_MARK = re.compile('.*(rufe.*die Frage|zur Frage|der Tagesordnung|Tagesordnungspunkt|Zusatzpunkt).*')
#POI_MARK = re.compile('\((.*)\)\s*$', re.M)
HEADER_MARK = re.compile(r'^(?:<poi_begin>)?\(.+?\)(?:\s+<poi_end>)?$|^Abgeordnetenhaus\s+von\s+Berlin$|^[0-9]{1,2}\.\s+Wahlperiode$')
#PAGENUMBER_MARK = re.compile(r'[0-9]{4}\s')
DATE_CAPTURE = re.compile(r'([0-9]{1,2}\.\s+.+\s+[0-9]{4})')

STATE = 'BE'

files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith("xml.txt")]

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

ls_speeches = []
ls_interjection_length = []
ls_text_length = []

debug = True

for filename in sorted(files):
    wp, session = int(filename[18:20]), int(filename[21:24])
    print(wp, session)

    # deletes existing entries for this state's election period and session. e.g SH 18, 001
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
            #import pdb; pdb.set_trace()
            print('date captured ' + date)
            date_captured = True
            continue
        elif not date_captured:
            continue
        elif not in_session and BEGIN_MARK.search(line):
            in_session = True
        elif not in_session:
            continue

        if HEADER_MARK.match(line.lstrip().rstrip()):
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
            if CHAIR_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip()):
                s = CHAIR_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip())
                new_speaker = s.group(2)
                president = True
                executive = False
                servant = False
                party = None
                role = 'chair'
            elif DATA_PROTECTION_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip()):
                s = DATA_PROTECTION_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip())
                new_speaker = s.group(1)
                president = False
                executive = False
                servant = False
                party = None
                role = 'data protection officer'
            elif LHR_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip()):
                s = LHR_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip())
                new_speaker = s.group(1)
                president = False
                executive = False
                servant = False
                party = None
                role = 'court of audit'
            elif OFFICIALS_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip()):
                s = OFFICIALS_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip())
                new_speaker = s.group(2)
                party = None
                president = False
                executive = False
                servant = True
                role = 'secretary'
            elif EXECUTIVE_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip()):
                s = EXECUTIVE_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip())
                new_speaker = s.group(2)
                new_speaker = re.sub(r'\s+\(Senatsverwaltung(?:\s+für(?:\s+(?:.+)?)?)?', '', new_speaker).replace(':', '')
                role = 'executive'
                party = None
                president = False
                executive = True
                servant = False
            elif SPEAKER_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip()) and not line.startswith('[') and not line.startswith('('):
                s = SPEAKER_MARK.match(line.replace('<poi_end>', '').lstrip().rstrip())
                new_speaker = s.group(1)
                president = False
                executive = False
                servant = False
                party = s.group(2)
                role = 'mp'
            else:
                if POI_ONE_LINER.match(line):
                    issue = POI_ONE_LINER.match(line).group(1)
                else:
                    issue = line
                    poi = True

            line = line.replace('<poi_end>', '')

        if not poi and current_speaker:
            new_speaker = new_speaker.replace('<poi_end>', '')
            if party:
              if 'LINKE' in party:
                party = 'DIE LINKE'
              elif 'Berichterstatter' in party:
                role = 'rapporteur of committee'
                party = 'SPD'
              elif party=='Grüne':
                party='GRÜNE'

        # if current_speaker and INTERJECTION_MARK.match(line) and not END_MARK.match(line):
        #     #import pdb; pdb.set_trace()
        #     interjection = True
        #     cnt_interjection += 1
        # if current_speaker and interjection:
        #     if INTERJECTION_END.search(line):
        #         interjection = None
        #         s = None
        #         continue
        #     else:
        #         s = None
        #         continue
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
                text = text.replace('<poi_begin>', '')
                if text and not interjection:
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
        if INTERJECTION_MARK.match(line.lstrip()) and not interjection and not '[zu Protokoll gegeben]' in line:
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
                text = text.replace('<poi_begin>', '')
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

            sub += 1
            interjection = True
            interjection_text = []

        if interjection:
            # import pdb; pdb.set_trace()
            if INTERJECTION_END.match(line.rstrip()):
                if current_speaker is not None:
                    line = line.replace('<poi_end>', '')
                    if line and not WHITE_SPACE_ONLY.match(line):
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
                                           'executive': current_executive,
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
                line = line.replace('<poi_end>', '')
                if line and not WHITE_SPACE_ONLY.match(line):
                    interjection_text.append(line)
                interjection_length += 1
                continue
   
        if current_speaker is not None:
            if interjection_complete:
                interjection_complete = None
                text = []
                line = line.replace('<poi_end>', '')
                if line and not WHITE_SPACE_ONLY.match(line):
                    line = helper.cleans_line_by(line)
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
                line = line.replace('<poi_end>', '')
                if line and not WHITE_SPACE_ONLY.match(line):
                    line = helper.cleans_line_by(line)
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
            # else:
            #     line = ''
            #     dotNotOnFirstLine = True
            text = []
            line = line.replace('<poi_end>', '')
            if line and not WHITE_SPACE_ONLY.match(line):
                line = helper.cleans_line_by(line)
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
    idx_txt = [i for i, e in enumerate(ls_text_length[0:10]) if e[0] > 15]

    pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()