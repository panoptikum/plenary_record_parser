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

log = logging.getLogger(__name__)

os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')
# os.chdir('/home/felix/privat/plenarprotokolle/code')

from lib import helper

locale.setlocale(locale.LC_TIME, "de_DE.utf-8")

STATE = 'TH'

DATA_PATH = os.environ.get('DATA_PATH', '../data/' + STATE)

# regular expressions to capture speeches of one session
BEGIN_STRING = r'^(?:Beginn(?::)?\s+[0-9]{1,2}(?:\.|:)[0-9]{1,2}\s+Uhr|Die\s+Sitzung\s+wird\s+um|Beginn\s+der\s+Sitzung)'
END_STRING = r'(?:Ende:\s+[0-9]{1,2}\.[0-9]{1,2}\s+Uhr|E\s+n\s+d\s+e\s+d\s+e\s+r\s+S\s+i\s+t\s+z\s+u\s+n\s+g)'
CHAIR_STRING = r'^<poi_begin>(Alterspräsident(?:in)?|Präsident(?:in)?|Vizepräsident(?:in)?)\s+(.+?):'
SPEAKER_STRING = r'^<poi_begin>(?:Abgeordnete(?:r)?)\s+(.+)\,\s+(.+?:|BÜNDNIS)'
EXECUTIVE_STRING = r'^<poi_begin>(.+?),\s+(Ministerpräsident(?:in)?:|Minister(?:in)?\s+für\s+.+(?::)?|Justizminister(?:in)?:|Finanzminister(?:in)?:|Innenminister(?:in)?:)'
OFFICIALS_STRING = r'^<poi_begin>(.+),\s+(Staatssekretär(?:in)?):'

# compilation of regular expressions
# advantage combination of strings is possible
BEGIN_MARK = re.compile(BEGIN_STRING)
END_MARK = re.compile(END_STRING)
CHAIR_MARK = re.compile(CHAIR_STRING)
SPEAKER_MARK = re.compile(SPEAKER_STRING)
EXECUTIVE_MARK = re.compile(EXECUTIVE_STRING)
OFFICIALS_MARK = re.compile(OFFICIALS_STRING)
SPEECH_ENDS = re.compile("|".join([CHAIR_STRING, SPEAKER_STRING, EXECUTIVE_STRING, OFFICIALS_STRING]))
INTERJECTION_MARK = re.compile(r'^<interjection_begin>\(')
INTERJECTION_END = re.compile(r'\)$')
HEADER_MARK = re.compile(r'Thüringer\sLandtag\s-\s[0-9]\.\sWahlperiode')
HEADER_SPEAKER_MARK = re.compile(r'\((?:Abg\.)|\(Alterspräsident(?:in)?|\(Präsident(?:in)?|\(Vizepräsident(?:in)?|\(Staatssekretär(?:in)?|\(Minister(?:in)?|\(Justizminister(?:in)?:|\(Finanzminister(?:in):|\(Innenminister(?:in)?|\(Ministerpräsident(?:in)?')
DATE_CAPTURE = re.compile(r'([0-9]{2}\.[0-9]{2}\.[0-9]{4})')
POI_ONE_LINER = re.compile(r'(.+?)?<poi_end>(?:.+)?')

files = sorted([os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith("xml.txt")])

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

ls_speeches = []
ls_interjection_length = []
ls_text_length = []
ls_interjection_length = []

for filename in files[58:]:

    filename_splitted = filename.split('_')
    # extracts wp, session no. and if possible date of plenary session
    wp, session = int(filename_splitted[1]), int(filename_splitted[2])
    date = None

    print(wp, session)

    # table.delete(wp=wp, session=session, state=STATE)

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
    poi_prev = False
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

    endend_with_interjection = False

    # contains list of dataframes, one df = one speech
    speeches = []

    for line, has_more in helper.lookahead(lines):
        #if '<interjection_begin>Beifall im ganzen Hause)<interjection_end>' in line:
        #    import pdb; pdb.set_trace()

        #pdb.set_trace()
        # to avoid whitespace before interjections; like ' (Heiterkeit bei SPD)'
        line = line.lstrip()

        # grabs date, goes to next line until it is captured
        if not date_captured and DATE_CAPTURE.search(line):
            date = DATE_CAPTURE.search(line).group(1)
            try:
                date = datetime.strptime(date, '%d. %B %Y').strftime('%Y-%m-%d')
            except ValueError:
                date = datetime.strptime(date, '%d.%m.%Y').strftime('%Y-%m-%d')
            print('date captured ' +  date)
            date_captured = True
            continue
        elif not date_captured:
            continue
        if not in_session and BEGIN_MARK.search(line):
            #import pdb; pdb.set_trace()
            print('now in session')
            in_session = True
            continue
        elif not in_session:
            continue

        #ignores header lines and page numbers e.g. 'Landtag Mecklenburg-Vorpommer - 6. Wahlperiode [...]'
        if line.replace('<interjection_begin>', '').replace('<interjection_end>', '').strip().isdigit():
           continue

        if poi:
            if POI_ONE_LINER.match(line):
                if POI_ONE_LINER.match(line).group(1):
                    issue = issue + ' ' + POI_ONE_LINER.match(line).group(1)
                issue = issue.replace('<poi_begin>', '')
                issue = issue.replace('<poi_end>', '')
                poi = False
                # poi_prev = True
                line = line.replace('<poi_end>', '')
            else:
                issue = issue + ' ' + line

        # detects speaker, if no interjection is found:
        if '<poi_begin>' in line:
            if CHAIR_MARK.match(line):
                s = CHAIR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2))
                president = True
                executive = False
                servant = False
                party = None
                role = 'chair'
                poi_prev = False
            elif EXECUTIVE_MARK.match(line):
                s = EXECUTIVE_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                role = 'executive'
                party = None
                president = False
                executive = True
                servant = False
                poi_prev = False
            elif OFFICIALS_MARK.match(line):
                s = OFFICIALS_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                party = None
                president = False
                executive = False
                servant = True
                role = 'state secretary'
                poi_prev = False
            elif SPEAKER_MARK.match(line):
                s = SPEAKER_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1)).rstrip(')').rstrip('*')
                president = False
                executive = False
                servant = False
                party = s.group(2)
                role = 'mp'
                poi_prev = False
            else:
                if POI_ONE_LINER.match(line):
                    issue = POI_ONE_LINER.match(line).group(1)
                    issue = issue.replace('<poi_begin>', '')
                    issue = issue.replace('<poi_end>', '')
                    # poi_prev = True
                # elif poi_prev:
                 #   issue = issue + ' ' + line
                  #  poi = True
                else:
                    issue = line
                    poi = True

            if new_speaker:
                new_speaker = new_speaker.replace(':', '').replace('<poi_end>', '').replace('<poi_begin>', '').strip()
                if party:
                    party = party.replace('BÜNDNIS 90', 'BÜNDNIS 90/DIE GRÜNEN')
                    if 'NKE' in party:
                        party = 'DIE LINKE'
                    elif 'BÜNDNIS' in party:
                        party = 'GRÜNE'
                    party = party.replace(':', '')

        # saves speech, if new speaker is detected:
        if s is not None and current_speaker is not None:
            # ensures that new_speaker != current_speaker or matches end of session, document
            if (new_speaker!=current_speaker or END_MARK.search(line) or not has_more) and not interjection:
                text_length = len(text)
                # joins list elements that are strings
                text = ''.join(text)
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                # removes whitespaces at the beginning and end
                text = text.strip()
                # # 
                #text = re.sub('-(?=[a-z])', '', text)
                text = text.replace('<interjection_begin>', '').replace('<interjection_end>', '')
                text = text.replace('<poi_begin>', '').replace('<poi_end>', '')

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
                    ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])
                # stops iterating over lines, if end of session is reached e.g. Schluss: 17:16 Uhr
                # to know order of speech within a given plenary session
                seq += 1
                # resets sub counter (for parts of one speech: speakers' parts and interjections)
                # for next speech
                sub = 0
                # resets current_speaker for next speech
                current_speaker = None
            elif (new_speaker!=current_speaker or END_MARK.search(line) or not has_more) and interjection:
                endend_with_interjection = True
                # to know order of speech within a given plenary session
                seq += 1
                # resets sub counter (for parts of one speech: speakers' parts and interjections)
                # for next speech
                sub = 0
            if END_MARK.search(line):
                in_session = False
                break
        # adds interjections to the data in such a way that order is maintained
        if INTERJECTION_MARK.match(line) and not interjection and not '<poi_begin>' in line:

        # skips lines that start with brackes for abbreviations at the beginning of line e.g. '(EU) Drucksache [...]'
            # variable contains the number of lines an interjection covers
            interjection_length = 0
            # saves speech of speaker until this very interjection
            if not interjection_complete and current_speaker is not None:
                text_length = len(text)
                # joins list elements of strings
                text = ''.join(text)
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                # removes whitespaces at the beginning and end
                text = text.strip()
                text = re.sub('-(?=[a-z])', '', text)
                text = text.replace('<interjection_begin>', '').replace('<interjection_end>', '')
                text = text.replace('<poi_begin>', '').replace('<poi_end>', '')
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
                    ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])

            #
            sub += 1
            interjection = True
            interjection_text = []
    # special case: interjection
        if interjection:
            # either line ends with ')' and opening and closing brackets are equal or we had two empty lines in a row
            if not '<interjection_begin>' in line and line and not line.isspace() or INTERJECTION_END.search(line):
                # to avoid an error, if interjection is at the beginning without anybod have started speaking
                # was only relevant for bavaria so far.
                if current_speaker is not None:
                    if INTERJECTION_END.search(line):
                        interjection_text.append(line)
                    # interjection_text.append(line)
                    interjection_text = [i.replace('-', '').rstrip() if i.rstrip().endswith('-') else i + ' ' for i in interjection_text]
                    interjection_text = ''.join(interjection_text)
                    # if 'Das sind aber zwei' in interjection_text:
                       # import pdb; pdb.set_trace()
                    # removes whitespace duplicates
                    interjection_text = re.sub(' +', ' ', interjection_text)
                    # removes whitespaces at the beginning and end
                    interjection_text = interjection_text.strip()
                    interjection_text = re.sub('-(?=[a-z])', '', interjection_text)
                    interjection_text = interjection_text.replace('<interjection_begin>', '').replace('<interjection_end>', '')
                    interjection_text = interjection_text.replace('<poi_begin>', '').replace('<poi_end>', '')
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
                cnt_brackets_opening = 0
                cnt_brackets_closing = 0
            else:
                line = line.replace('<interjection_begin>', '').replace('<interjection_end>', '')
                if line and not line.isspace():
                    interjection_text.append(line)
                    interjection_length += 1
                continue
        if current_speaker is not None and not endend_with_interjection:
            if interjection_complete:
                interjection_complete = None
                text = []
                if line and not line.isspace() and not INTERJECTION_END.search(line):
                    line = helper.cleans_line(line)
                    text.append(line)
                continue
            else:
                current_role = current_role.strip()

                line = helper.cleans_line(line)
                if line and not line.isspace():
                    text.append(line)
                continue

        if s is not None:
            if not endend_with_interjection:
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
            endend_with_interjection = False
            interjection_complete = None
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
idx_txt = [i for i, e in enumerate(ls_text_length) if e[0] > 250]

pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()