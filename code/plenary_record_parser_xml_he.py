# coding: utf-8
import os
import locale
import re
import logging
# import requests
import dataset
# from lxml import html
# from urlparse import urljoin
import pandas as pd
# import json
from datetime import datetime
import numpy as np

os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')
#print(os.getcwd())

from lib import helper

log = logging.getLogger(__name__)

locale.setlocale(locale.LC_TIME, "de_DE.utf-8")

STATE = 'HE'

DATA_PATH = os.environ.get('DATA_PATH', '../data/' + STATE)

# with open("HE/deputies_hessen_18.json", encoding="utf-8") as file:
#     speakers_wp18=json.loads(file.read())

# abg_wp18 = []

# for person in speakers_wp18['profiles']:
#     abg_wp18.append(person['personal']['last_name'])

# with open("HE/deputies_hessen_19.json", encoding="utf-8") as file:
#     speakers_wp19=json.loads(file.read())

# abg_wp19 = []

# for person in speakers_wp19['profiles']:
#     abg_wp19.append(person['personal']['last_name'])

# regular expressions to capture speeches of one session
BEGIN_STRING = r'^(?:<interjection_begin)?\(Beginn:?\s+(?:[0-9]{1,2}[.:][0-9]{1,2}|[0-9]{1,2})\s+Uhr'
END_STRING = r'^(?:<interjection_begin)?(?:\(((?:Allgemeiner\s+)?Beifall\s+–\s+|Heiterkeit\s+und\s+Beifall\s+–\s+)?|GRÜNEN\s+und\s+der\s+LINKEN\s+–\s+)?Schluss:?\s+(?:[0-9]{1,2}[.:][0-9]{1,2}|[0-9]{1,2})\s+Uhr\)'
CHAIR_STRING = r'^(Alterspräsident(?:in)?|Präsident?(?:in)?|Erste(?:r)?\s+[Vv]izepräsident(?:in)?|Vizepräsident(?:in)?)\s+(.+)(?:\s+\((?:fortfahrend|unterbrechend)\))?'
SPEAKER_STRING = r'^(.+?)(?:\s+\(.+?\))?\s+\((fraktionslos|CDU|SPD|FDP|DIE\s+LINKE|(?:Taunus\s+/)?BÜNDNIS(?:\s+90/DIE\s+GRÜ(?:-|NEN)?|.+))\)?'
EXECUTIVE_STRING = r'^(.+?),\s+(Finanzminister(?:in)?|Sozialminister(?:in)?|Kultusminister(?:in)?|Minister(?:in)?(?:\s+)?(?:für|und\s+Chef|des\s+Innern|der\s+(?:Finanzen|Justiz)).+|Ministerpräsident(?:in)?:)'
OFFICIALS_STRING = r'^(.+?),\s+(Staatssekretär(?:in)?\s+(?:im|für|sowie).+)'
CHAIRMAN_STRING = r'^(.+?),\s+(Vorsitzender?(?:\s+der\s+Enquetekommission|\s+des\s+Haushaltsausschus-)?)'

# compilation of regular expressions
# advantage combination of strings is possible
BEGIN_MARK = re.compile(BEGIN_STRING)
END_MARK = re.compile(END_STRING)
CHAIR_MARK = re.compile(CHAIR_STRING)
SPEAKER_MARK = re.compile(SPEAKER_STRING)
EXECUTIVE_MARK = re.compile(EXECUTIVE_STRING)
OFFICIALS_MARK = re.compile(OFFICIALS_STRING)
RAPPORTEUR_MARK = re.compile(r'^(.+?),\s+Berichterstatter(?:in)?')
DATA_PROTECTION_MARK = re.compile(r'^(.+?),\s+(?:Hessischer\s+)?Datenschutz(?:beauftragter)?')
STATE_ATTORNEY_MARK = re.compile(r'^(.+?),(?:\s+stellvertretender?)?\s+Landesanw[aä]lt(?:in)?')
STATE_COURT_MARK = re.compile(r'^(.+?),\s+(?:(?:[Vv]ize)?[pP]räsident(?:in)?\s+des\s+Staatsge(?:richts-|-|richtshofs)|nicht\s+richterliches)')
MAYOR_MARK = re.compile(r'^(.+?),\s+(?:Ober)?[Bb]ürgermeister(?:in)?')
CHAIRMAN_MARK = re.compile(CHAIRMAN_STRING)
RECORDING_CLERK_MARK = re.compile(r'(.+?),\s+Schriftführer(?:in)?:')
#SPEECH_ENDS = re.compile("|".join([CHAIR_STRING, SPEAKER_STRING, EXECUTIVE_STRING, OFFICIALS_STRING]))
INTERJECTION_MARK = re.compile(r'^\(')
INTERJECTION_END = re.compile(r'\)$')
HEADER_MARK = re.compile(r'^Hessischer\s+Landtag\s+·\s+[0-9]{1,2}\.\s+Wahlperiode')
#HEADER_SPEAKER_MARK = re.compile(r'\((?:Abg\.)|\(Alterspräsident(?:in)?|\(Präsident(?:in)?|\(Vizepräsident(?:in)?|\(Staatssekretär(?:in)?|\(Minister(?:in)?|\(Justizminister(?:in)?:|\(Finanzminister(?:in):|\(Innenminister(?:in)?|\(Ministerpräsident(?:in)?')
DATE_CAPTURE = re.compile(r'([0-9]{1,2}\.\s+[0-9]{1,2}\.\s+[0-9]{4})')
NO_INTERJECTION = re.compile(r'^.{1,6}[\)]')
POI_ONE_LINER = re.compile(r'(.+?)?<poi_end>(?:.+)?')

hessentag_people = ['Anne Weihrich', 'Markus Glanzner', 
                    'Rebecca Ross', 'Andreas Richhardt', 
                    'Janina Till', 'Cetin Celik', 
                    'Selma Kücükyavuz', 'Marcel Sedlmayer', 
                    'Matthias Mücke', 'Julia Tanzer', # 2009
                    'Mona Lorena Monzien', 'Fabian Gies', # 2010
                    'Charmaine Weisenbach', 'Christian Peter',
                    'Nina Becker', 'Florian Köhler', # 2012
                    'Tobias Krechel', 'Alexandra Berge',
                    'Lisa-Marie Fritzsche', 'Lukas Goos'] 

files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith("xml.txt")]

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

ls_speeches = []
ls_interjection_length = []
ls_text_length = []

ls_interjection_length = []

for filename in files[:116]+files[119:]:

    wp, session, date = int(filename[18:20]), int(filename[21:26]), None
    print(wp, session)

    # table.delete(wp=wp, session=session, state=STATE)
    # if wp==18:
    #     abg = abg_wp18
    # elif wp==19:
    #     abg = abg_wp19

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
        # if in_session:
        #     import pdb; pdb.set_trace()
        if not date_captured and DATE_CAPTURE.search(line):
            date = DATE_CAPTURE.search(line).group(1)
            date = datetime.strptime(date, '%d. %m. %Y').strftime('%Y-%m-%d')
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

        if poi:
            if POI_ONE_LINER.match(line):
                if POI_ONE_LINER.match(line).group(1):
                    issue = issue + ' ' + POI_ONE_LINER.match(line).group(1)
                poi = False
                line = line.replace('<poi_end>', '')
            else:
                issue = issue + ' ' + line

        #ignores header lines and page numbers
        if '<poi_begin>' in line and not poi:
            line = line.replace('<poi_begin>', '')
            if STATE_COURT_MARK.match(line):
                s = STATE_COURT_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                party = None
                president = False
                executive = False
                servant = False
                role = 'state court of law'
            elif CHAIR_MARK.match(line):
            #import pdb; pdb.set_trace()
                s = CHAIR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2))
                president = True
                executive = False
                servant = False
                party = None
                role = 'chair'
            elif EXECUTIVE_MARK.match(line):
                s = EXECUTIVE_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                role = 'executive'
                party = None
                president = False
                executive = True
                servant = False
            elif OFFICIALS_MARK.match(line):
                s = OFFICIALS_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                party = None
                president = False
                executive = False
                servant = True
                role = 'secretary'
            elif SPEAKER_MARK.match(line):
                s = SPEAKER_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = s.group(2)
                role = 'mp'
            elif RAPPORTEUR_MARK.match(line):
                s = RAPPORTEUR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                party = None
                president = False
                executive = False
                servant = False
                role = 'rapporteur'
            elif DATA_PROTECTION_MARK.match(line) or 'Michael Ronellenfitsch' in line:
                s = DATA_PROTECTION_MARK.match(line)
                if DATA_PROTECTION_MARK.match(line):
                    new_speaker = re.sub(' +', ' ', s.group(1))
                else:
                    new_speaker = 'Michael Ronellenfitsch'
                party = None
                president = False
                executive = False
                servant = False
                role = 'data protection officer'
            elif STATE_ATTORNEY_MARK.match(line):             
                s = STATE_ATTORNEY_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                party = None
                president = False
                executive = False
                servant = False
                role = 'state attorney'
            elif CHAIRMAN_MARK.match(line):
                s = CHAIRMAN_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                party = None
                president = False
                executive = False
                servant = False
                role = 'chairman of inquiry'
            elif MAYOR_MARK.match(line):
                s = MAYOR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                party = None
                president = False
                executive = False
                servant = False
                role = 'mayor'
            elif any(e in line for e in hessentag_people):
                new_speaker = line
                party = None
                president = False
                executive = False
                servant = False
                role = 'couple of Hessentag'
            elif RECORDING_CLERK_MARK.match(line):
                s = RECORDING_CLERK_MARK.match(line)
                new_speaker = s.group(1)
                party = None
                president = False
                executive = False
                servant = False
                role = 'recording clerk'
            else:
                if POI_ONE_LINER.match(line):
                    issue = POI_ONE_LINER.match(line).group(1)
                else:
                    issue = line
                    poi = True

            line = line.replace('<poi_end>', '')

        if not poi and current_speaker:
            if new_speaker:            
                new_speaker = new_speaker.replace('<poi_end>', '').replace(':', '').strip()
        # if new_speaker=='nung ist jetzt möglich! - Adelheid Rupp':
        #     import pdb; pdb.set_trace()
        if s is not None and current_speaker is not None:
            #import pdb; pdb.set_trace()
            if new_speaker!=current_speaker or END_MARK.search(line) or not has_more:
                # concatenates lines to one string
                #text = [i + ' ' if not i.endswith('-') else i for i in text]
                text = ''.join(text).replace('<poi_end>', '')
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
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
        # adds interjections to the data in such a way that order is maintained
        if '<interjection_begin' in line:
            line = line.replace('<interjection_begin', '')
            if INTERJECTION_MARK.match(line) and not interjection:
                # skips lines that simply refer to the current speaker
                if not NO_INTERJECTION.match(line):
                    # if current_speaker in line:
                    #     continue
                    #text = [i + ' ' if not i.endswith('-') else i for i in text]
                     # concatenates lines to one string
                    interjection_length = 0
                    if not interjection_complete and current_speaker is not None:
                        #import pdb; pdb.set_trace()
                        text = [i for i in text]
                        text = ''.join(text).replace('<poi_end>', '')
                        # removes whitespace duplicates
                        text = re.sub(' +', ' ', text)
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
                    #import pdb; pdb.set_trace()
                    sub += 1
                    interjection = True
                    interjection_text = []
        if interjection:
            cnt_brackets_opening += line.count('(')
            cnt_brackets_closing += line.count(')')
            #import pdb; pdb.set_trace()
            if '<interjection_end>' in line:
                line = line.replace('<interjection_end>', '')
                if current_speaker is not None:
                    interjection_text.append(line)
                    interjection_text = [i + ' ' if not i.endswith('-') else i.replace('-', '') for i in interjection_text]
                    interjection_text = ''.join(interjection_text)
                    # removes whitespace duplicates
                    interjection_text = re.sub(' +', ' ', interjection_text)
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
                interjection_text.append(line)
                interjection_length += 1
                continue
        if current_speaker is not None:
            if interjection_complete:
                interjection_complete = None
                text = []
                line = helper.cleans_line(line)
                text.append(line)
                continue
            else:
                current_role = current_role.strip()
                line = helper.cleans_line(line)
                text.append(line)
                continue

        if s is not None:
            if ":* " in line:
                line = line.split(':* ', 1)[-1]
            elif ":" in line:
                line = line.split(':', 1)[-1]
            line = helper.cleans_line(line)
            text = []
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
idx = [i for i, e in enumerate(ls_interjection_length) if e[0] >= 8 and e[0]<10]

#text length
idx_txt = [i for i, e in enumerate(ls_text_length[0:10]) if e[0] > 15]

pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()

pd_speeches['interjection_prev'] = pd_speeches.interjection.shift(1)
pd_speeches['interjection_successive'] = np.where((pd_speeches['interjection']) & (pd_speeches['interjection_prev']), True, False)