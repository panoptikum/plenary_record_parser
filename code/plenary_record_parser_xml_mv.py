# coding: utf-8
import os
import re
import logging
#import requests
import dataset
from lxml import html
#from urlparse import urljoin
import pandas as pd
import json
from datetime import datetime

log = logging.getLogger(__name__)

os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')

from lib import helper
DATA_PATH = os.environ.get('DATA_PATH', '../data/MV')

# with open("data/deputies_hessen_18.json", encoding="utf-8") as file:
#     speakers_wp18=json.loads(file.read())

# abg_wp18 = []

# for person in speakers_wp18['profiles']:
#     abg_wp18.append(person['personal']['last_name'])

# with open("data/deputies_hessen_19.json", encoding="utf-8") as file:
#     speakers_wp19=json.loads(file.read())

# abg_wp19 = []

# for person in speakers_wp19['profiles']:
#     abg_wp19.append(person['personal']['last_name'])
long_lastnames = ['Al-Sabty']

# regular expressions to capture speeches of one session
BEGIN_STRING = r'^(?:(?:<interjection_begin>)?<poi_begin>)?Beginn:?\s+(?:[0-9]{1,2}[.:][0-9]{1,2}|[0-9]{1,2})\s+Uhr'
END_STRING = r'^(?:<interjection_begin>)?(?:<poi_begin>)?Schluss:?\s+(?:[0-9]{1,2}[.:][0-9]{1,2}|[0-9]{1,2})\s+Uhr'
CHAIR_STRING = r'^(Alterspräsident(?:in)?|Präsident(?:in)?|Erste(?:r)?\s+Vizepräsident(?:in)?|Vizepräsident(?:in)?)(?:<poi_end>)?\s+(.+?)(?:\s+\((?:fortfahrend|unterbrechend)\))?:?<poi_end>'
SPEAKER_STRING = r'^(.+)(?:<poi_end>,|,<poi_end>)\s+(?:<poi_end>)?(CDU|SPD|BÜNDNIS\s+90/DIE\s+GRÜNEN|DIE\s+LINKE|NPD|fraktionslos|(?:Freie\s+Wähler/)?BMV|AfD)'
EXECUTIVE_STRING = r'^(Ministerpräsident(?:in)?|Minister(?:in)?)\s+(.+?)<poi_end>'
OFFICIALS_STRING = r'^(Staatssekretär(?:in)?)\s+(.+)(?:<poi_end>|:)'

# compilation of regular expressions
# advantage combination of strings is possible
BEGIN_MARK = re.compile(BEGIN_STRING)
END_MARK = re.compile(END_STRING)
CHAIR_MARK = re.compile(CHAIR_STRING)
SPEAKER_MARK = re.compile(SPEAKER_STRING)
EXECUTIVE_MARK = re.compile(EXECUTIVE_STRING)
OFFICIALS_MARK = re.compile(OFFICIALS_STRING)
CONSTITUTIONAL_COURT_MARK = re.compile('(Burkhard Thiele|Sven Nickels|Dr. Ulrike Lehmann-Wandschneider|Konstantin Tränkmann|Dr. Claus Dieter Classen|Barbara Borchardt)')
OMBUDSMAN_MARK = re.compile(r'^Bürgerbeauftragter?\s+(.+?)<poi_end>')
FEDERAL_COMISSIONER_MARK = re.compile(r'(?:Landesbeauftragter?\s+für\s+Mecklenburg-Vorpommern\s+für\s+die\s+Unterlagen\s+des\s+Staatssicherheitsdienstes\s+der\s+ehemaligen\s+DDR\s+)?(Anne Drescher|Jörn Mothes)')
#SPEECH_ENDS = re.compile("|".join([CHAIR_STRING, SPEAKER_STRING, EXECUTIVE_STRING, OFFICIALS_STRING]))
INTERJECTION_MARK = re.compile(r'^\(')
INTERJECTION_END = re.compile(r'\)$')
POI_ONE_LINER = re.compile(r'(.+?)?<poi_end>(?:.+)?')
ONLY_POI = re.compile(r'^<poi_begin>(\s+)?$')
HEADER_MARK = re.compile(r'^Landtag\s+Mecklenburg\-Vorpommern\s+\–\s+[0-9]{1,2}\.\s+Wahlperiode')
#HEADER_SPEAKER_MARK = re.compile(r'\((?:Abg\.)|\(Alterspräsident(?:in)?|\(Präsident(?:in)?|\(Vizepräsident(?:in)?|\(Staatssekretär(?:in)?|\(Minister(?:in)?|\(Justizminister(?:in)?:|\(Finanzminister(?:in):|\(Innenminister(?:in)?|\(Ministerpräsident(?:in)?')
DATE_CAPTURE = re.compile(r'([0-9]{1,2}\.[0-9]{1,2}\.[0-9]{4})')
NO_INTERJECTION = re.compile(r'^.{1,6}[\)]')

STATE = 'MV'

files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith('xml.txt')]

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

ls_interjection_length = []
ls_text_length = []
ls_speeches = []

table.delete(state=STATE)

for filename in files:

    # extracts wp, session no. and if possible date of plenary session
    wp, session, date = int(filename[17:19]), int(filename[20:24]), None
    print(wp, session)

    if wp==18:
        abg = abg_wp18
    elif wp==19:
        abg = abg_wp19

    with open(filename, 'rb') as fh:
        text = fh.read().decode('utf-8')

    base_data = {
        'filename': filename,
        'sitzung': session,
        'wahlperiode': wp,
        'state': 'sachen-anhalt'
    }

    print("Loading transcript: %s/%.3d, from %s" % (wp, session, filename))

    lines = text.split('\n')

       # trigger to skip lines until date is captured
    date_captured = False
    # trigger to skip lines until in_session mark is matched
    in_session = False

    # poi
    poi = False
    issue = None
    current_issue = None
    concat_issues = False
    finished_poi = False


    # variable captures contain new speaker if new speaker is detected
    new_speaker = None
    # contains current speaker, to use actual speaker and not speaker that interrupts speech
    current_speaker = None
    s = None

    # trigger to check whether a interjection is found
    interjection = False
    interjection_complete = None
    missing_closing = False
    end_of_interjection = False

    # identation
    identation = False

    # trigger to find parts where zwischenfragen are continued
    # without labelling current speaker
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
        # to avoid whitespace before interjections; like ' (Heiterkeit bei SPD)'
        # line = line.lstrip()

        # grabs date, goes to next line until it is captured
        if not date_captured and DATE_CAPTURE.search(line):
            date = DATE_CAPTURE.search(line).group(1)
            date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
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
                finished_poi = True
            else:
                issue = issue + ' ' + line

        #ignores header lines and page numbers e.g. 'Landtag Mecklenburg-Vorpommer - 6. Wahlperiode [...]'
        # detects speaker, if no interjection is found:
        if ('<poi_begin>' in line and not ONLY_POI.match(line) or '<poi_end>' in line) and not poi:
            line = line.replace('<poi_begin>', '')
            if CHAIR_MARK.match(line):
                s = CHAIR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2)).replace(':', '').strip()
                president = True
                executive = False
                servant = False
                party = None
                role = 'chair'
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
                role = 'secretary'
            elif SPEAKER_MARK.match(line):
                s = SPEAKER_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = s.group(2)
                role = 'mp'
            elif any([e in line for e in long_lastnames]):
                if 'Al-Sabty<poi_end>' in line:
                    new_speaker = 'Dr. Hikmat Al-Sabty'
                    president = False
                    executive = False
                    servant = False
                    party = 'DIE LINKE'
                    role = 'mp'
            elif OMBUDSMAN_MARK.match(line):
                s = OMBUDSMAN_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = None
                role = 'ombudsman'
            elif FEDERAL_COMISSIONER_MARK.search(line):
                s = FEDERAL_COMISSIONER_MARK.search(line)
                new_speaker = s.group(1)
                president = False
                executive = False
                servant = False
                party = None
                role = 'federal commissioner for the documents from the Stasi'
                if new_speaker == 'Jörn Mothes':
                    role == 'federal advisor in the council for the documents from the Stasi'
            elif CONSTITUTIONAL_COURT_MARK.search(line):
                s = CONSTITUTIONAL_COURT_MARK.search(line)
                new_speaker = s.group(1)
                president = False
                executive = False
                servant = False
                party = None
                role = 'member of the federal constitutional court'
            else:
                if finished_poi:
                    finished_poi = False
                elif POI_ONE_LINER.match(line):
                    issue = POI_ONE_LINER.match(line).group(1)
                else:
                    issue = line
                    poi = True
                if 'Drucksache' in issue:
                    current_issue = issue

            line = line.replace('<poi_end>', '')

        if not poi and current_speaker:
            if new_speaker:
                new_speaker = (new_speaker
                               .replace('<poi_end>', '')
                               .replace('(fortfahrend)', '')
                               .replace('*', '')
                               .replace(')', '')
                               .replace(':', '')
                               .strip()
                               )
                new_speaker = re.sub(' +', ' ', new_speaker)

        # saves speech, if new speaker is detected:
        if s is not None and current_speaker is not None:
            # ensures that new_speaker != current_speaker or matches end of session, document
            if new_speaker!=current_speaker or END_MARK.search(line) or not has_more:
                if interjection:
                    interjection_text = ' '.join(interjection_text)
                    # removes whitespace duplicates
                    interjection_text = re.sub(' +', ' ', interjection_text)
                    # removes whitespaces at the beginning and end
                    interjection_text = (interjection_text.replace('<interjection_begin>', '')
                                                          .replace('<interjection_end>', '')
                                                          )
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
                                       'issue': current_issue})
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
                                       'issue': current_issue}

                    table.insert(speech_dict)
                    interjection_length += 1
                    ls_interjection_length.append([interjection_length, wp, session, seq, sub, current_speaker, interjection_text])
                    interjection = False
                    text = []
                # joins list elements that are strings
                text_length = len(text)
                text = ''.join(text)

                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                text = text.replace('<poi_end>', '')
                text = text.replace('<poi_begin>', '')
                text = text.replace('<interjection_begin>', '')
                text = text.replace('<interjection_end>', '')
                # removes whitespaces at the beginning and end
                text = text.strip()
                # # 
                # text = re.sub('-(?=[a-z])', '', text)
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
                                           'issue': current_issue})
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
                                   'issue': current_issue}

                    table.insert(speech_dict)
                    ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])
                # stops iterating over lines, if end of session is reached e.g. Schluss: 17:16 Uhr
                if END_MARK.search(line):
                    in_session = False
                    break
                # to know order of speech within a given plenary session
                seq += 1
                # resets sub counter (for parts of one speech: speakers' parts and interjections)
                # for next speech
                sub = 0
                # resets current_speaker for next speech
                current_speaker = None
        # adds interjections to the data in such a way that order is maintained
        if '<interjection_begin>' in line and not interjection:
            if INTERJECTION_MARK.match(line.replace('<interjection_begin>', '')):
                if '<interjection_end>' in line:
                    every_line = True
                else:
                    every_line = False
            # skips lines that start with brackes for abbreviations at the beginning of line e.g. '(EU) Drucksache [...]'
                # variable contains the number of lines an interjection covers
                interjection_length = 0
                # saves speech of speaker until this very interjection
                if not interjection_complete and current_speaker is not None:
                    # joins list elements of strings
                    text_length = len(text)
                    text = ''.join(text)
                    # removes whitespace duplicates
                    text = re.sub(' +', ' ', text)
                    # removes whitespaces at the beginning and end
                    text = text.strip()
                    text = re.sub('-(?=[a-z])', '', text)
                    text = text.replace('<poi_end>', '')
                    text = text.replace('<poi_begin>', '')
                    text = text.replace('<interjection_begin>', '')
                    text = text.replace('<interjection_end>', '')
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
                                               'date': date,
                                               'issue': current_issue})
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
                                       'issue': current_issue}

                        table.insert(speech_dict)
                        ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])


                #
                sub += 1
                interjection = True
                interjection_text = []
        # special case: interjection
        if interjection:
            # signals end of interjection
            # either line ends with ')' and opening and closing brackets are equal or we had two empty lines in a row
            if line.isspace() or not line:
                continue
            elif not '<interjection_begin>' in line and not '<interjection_end>' in line and every_line or END_MARK.search(line):
                end_of_interjection = True
            elif '<interjection_end>' in line and not every_line:
                end_of_interjection = True
                # to avoid an error, if interjection is at the beginning without anybod have started speaking
                # was only relevant for bavaria so far.
            if end_of_interjection:
                if not every_line:
                    if line and not line.isspace():
                        interjection_text.append(line)
                if current_speaker is not None:
                    #interjection_text.append(line)
                    #interjection_text = [i + ' ' for i in interjection_text if not i.endswith('-')]
                    interjection_text = ' '.join(interjection_text)
                    # removes whitespace duplicates
                    interjection_text = re.sub(' +', ' ', interjection_text)
                    # removes whitespaces at the beginning and end
                    interjection_text = (interjection_text.replace('<interjection_begin>', '')
                                                          .replace('<interjection_end>', '')
                                                          )
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
                end_of_interjection = False
                if END_MARK.search(line):
                    break
                # continue
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
                if line and not line.isspace() and every_line:
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
pd_speeches.to_csv('mv_test.csv')


# checks
# interjection length
# idx = [i for i, e in enumerate(ls_interjection_length)
#        if e[0] >= 8 and e[0] < 10]

# #text length
# idx_txt = [i for i, e in enumerate(ls_text_length[0:10]) if e[0] > 15]

# pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()

# pd_speeches['interjection_prev'] = pd_speeches.interjection.shift(1)
# pd_speeches['interjection_successive'] = np.where((pd_speeches['interjection']) & (pd_speeches['interjection_prev']), True, False)

# pd_speeches.loc[(pd_speeches.role=='mp') & (pd_speeches.party.notna())].loc[:, ['speaker', 'party']]

# pd_speeches.loc[(pd_speeches.role=='mp') & (pd_speeches.party.notna()), ['speaker', 'party']].drop_duplicates().sort_values('speaker')