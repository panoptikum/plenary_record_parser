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
#os.chdir('/home/felix/privat/plenarprotokolle/code')

from lib import helper

log = logging.getLogger(__name__)

if os.getcwd()=='/home/felix/privat/plenarprotokolle/code':
    locale.setlocale(locale.LC_TIME, "de_DE.utf8")
else:
    locale.setlocale(locale.LC_TIME, "de_DE")

DATA_PATH = os.environ.get('DATA_PATH', '../data/SH')

ministers_wp18 = ['Torsten Albig', 'Robert Habeck', 'Monika Heinold', 'Anke Spoorendonk', 'Reinhard Meyer',
                  'Waltraud Wende', 'Andreas Breitner', 'Kristin Alheit']

ministers_wp19 = ['Daniel Günther', 'Sütterlin-Waack', 'Karin Prien', 'Hans-Joachim Grote', 
                  'Robert Habeck', 'Monika Heinold', 'Bernd Buchholz', 'Heiner Garg']

# regular expressions to capture speeches of one session
BEGIN_STRING = r'^Beginn:\s+(?:Beginn\s+)?[0-9]{1,2}[.:][0-9]{1,2}'
END_STRING = r'^Schluss:\s+[0-9]{1,2}[.:][0-9]{1,2}'
CHAIR_STRING = r'^(Alterspräsident(?:in)?|Präsident(?:in)?|Erste(?:r)?\s+Vizepräsident(?:in)?|Vizepräsident(?:in)?)\s+(Wolfgang\s+Kubicki|Klaus\s+Schlie|(?:Kirsten\s+)?Eickhoff-Weber|Oliver\s+Kumbartky|Rasmus\s+Andresen|Marlies\s+Fritzen|Annabell\s+Krämer|Bernd\s+Heinemann)(?:\s+\((?:fortfahrend|unterbrechend)\))?:'
SPEAKER_STRING = r'^(.+?)\s+\[(AfD|CDU|SPD|FDP|SSW|PIRATEN|BÜNDNIS\s+90/DIE(?:\s+GRÜ(?:\-|NEN|fraktionslos))?)\]?(?:,\s+Berichterstatter(?:in)?)?'
EXECUTIVE_STRING = r'^(?P<speaker1>.+?)\,\s+(Ministerpräsident(?:in)?:|Minister(?:in)?\s+für|Justizminister(?:in)?:|Finanzminister(?:in)?:|Innenminister(?:in)?)'
OFFICIALS_STRING = r'^Staatssekretär(?:in)?\s+(?P<speaker1>.+?):|^(?P<speaker2>.+?)\,\s+Staatssekretär(?:in)?'

# compilation of regular expressions
# advantage combination of strings is possible
BEGIN_MARK = re.compile(BEGIN_STRING)
END_MARK = re.compile(END_STRING)
CHAIR_MARK = re.compile(CHAIR_STRING)
SPEAKER_MARK = re.compile(SPEAKER_STRING)
EXECUTIVE_MARK = re.compile(EXECUTIVE_STRING)
OFFICIALS_MARK = re.compile(OFFICIALS_STRING)
#SPEECH_ENDS = re.compile("|".join([CHAIR_STRING, SPEAKER_STRING, EXECUTIVE_STRING, OFFICIALS_STRING]))
INTERJECTION_MARK = re.compile(r'^\(')
INTERJECTION_END = re.compile(r'\)$')
HEADER_MARK = re.compile(r'^Schleswig\-Holsteinischer\s+Landtag\s+\([0-9]{1,2}\.\s+WP\)')
FOOTER_LASTPAGE_MARK = re.compile(r'Herausgegeben\s+vom\s+Präsidenten\s+des\s+Schleswig\-Holsteinischen\s+Landtags\s+\-\s+Stenografischer\s+Dienst')
#HEADER_SPEAKER_MARK = re.compile(r'\((?:Abg\.)|\(Alterspräsident(?:in)?|\(Präsident(?:in)?|\(Vizepräsident(?:in)?|\(Staatssekretär(?:in)?|\(Minister(?:in)?|\(Justizminister(?:in)?:|\(Finanzminister(?:in):|\(Innenminister(?:in)?|\(Ministerpräsident(?:in)?')
DATE_CAPTURE = re.compile(r'([0-9]{1,2}\.\s+.+\s+[0-9]{4})')
NO_INTERJECTION = re.compile(r'^\([A-Za-z]{1,4}\)')
ZWISCHENFRAGE_ANTWORT = re.compile(r'^-\s')

STATE = 'SH'
ls_speeches = []
files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith("pp.txt")]

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

# debug mode
debug = False

for filename in files:

    # extracts wp, session no. and if possible date of plenary session
    wp, session = str(int(filename[11:13])), str(int(filename[14:17]))
    date = None

    print(str(wp), str(session))
       
    # deletes existing entries for this state's election period and session. e.g SH 18, 001
    if not debug:
      table.delete(wp=wp, session=session, state=STATE)

    if wp=='18':
        ministers = ministers_wp18
        # abg = abg_wp18
    elif wp=='19':
        ministers = ministers_wp19
        # abg = abg_wp19

    with open(filename, 'rb') as fh:
        text = fh.read().decode('utf-8')

    base_data = {
        'filename': filename,
        'sitzung': session,
        'wahlperiode': wp,
        'state': 'sachen-anhalt'
    }

    print("Loading transcript: %s/%s, from %s" % (wp, session, filename))

    lines = text.replace('\n', '\r').split('\r')

    # trigger to skip lines until date is captured
    date_captured = False
    # trigger to skip lines until in_session mark is matched
    in_session = False

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
        # if line=='nen und Entscheidungen die Menschen in unserem':
        #     import pdb; pdb.set_trace()
        # to avoid whitespace before interjections; like ' (Heiterkeit bei SPD)'
        line = line.lstrip()

        # grabs date, goes to next line until it is captured
        if not date_captured and DATE_CAPTURE.search(line):
            date = DATE_CAPTURE.search(line).group(1)
            date = datetime.strptime(date, '%d. %B %Y').strftime('%Y-%m-%d')
            print('date captured ' + date)
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
        if HEADER_MARK.match(line) or line.strip().isdigit() or FOOTER_LASTPAGE_MARK.match(line):
            continue

        # detects speaker, if no interjection is found:
        if not INTERJECTION_MARK.match(line) and not interjection:
            if CHAIR_MARK.match(line):
                s = CHAIR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2))
                president = True
                executive = False
                servant = False
                party = None
                role = 'chair'

                zwischenfrage=False
            elif EXECUTIVE_MARK.match(line):
                if any(e in line for e in ministers):
                    if not 'Herrn' in line and not 'Frau' in line:
                        if not 'Ministerin Anke Spoorendonk, Ministerin' in line and not 'Breitner, und Herr Reinhard Meyer' in line and not 'Wort. Herr Minister Reinhard Meyer' in line:
                            s = EXECUTIVE_MARK.match(line)
                            new_speaker = re.sub(' +', ' ', s.group('speaker1'))
                            president = False
                            executive = True
                            servant = False
                            party = None
                            role = 'executive'
            # elif OFFICIALS_MARK.match(line):
            #     s = OFFICIALS_MARK.match(line)
            #     import pdb; pdb.set_trace()
            #     if s.group('speaker2'):
            #         new_speaker = re.sub(' +', ' ', s.group('speaker2'))
            #     elif s.group('speaker1'):
            #         new_speaker = re.sub(' +', ' ', s.group('speaker1'))
            #     president = False
            #     executive = False
            #     servant = True
            #     party = None
            #     role = 'secretary'
            elif SPEAKER_MARK.match(line):
                if not INTERJECTION_END.match(line):
                    s = SPEAKER_MARK.match(line)
                    new_speaker = re.sub(' +', ' ', s.group(1)).rstrip('*')
                    president = False
                    executive = False
                    servant = False
                    party = re.sub(r'BÜNDNIS\s+90/DIE(?:\s+GRÜ(?:\-|NEN))?', 'GRÜNE', s.group(2)).split(':')[0].replace(']', '')
                    role = 'mp'
            #ensures contiunation with corret speaker if protocol only uses '-'
            elif zwischenfrage and speaker_cnt==2:
                if ZWISCHENFRAGE_ANTWORT.match(line):
                    # pdb; pdb.set_trace()
                    s = antwort_s
                    new_speaker = antwort_speaker
                    president = antwort_president
                    executive = antwort_executive
                    servant = antwort_servant
                    party = antwort_party
                    role = antwort_role

                    zwischenfrage==False

        # saves speech, if new speaker is detected:
        if s is not None and current_speaker is not None:
            # ensures that new_speaker != current_speaker or matches end of session, document
            if new_speaker!=current_speaker or END_MARK.search(line) or not has_more:
                text_length = len(text)
                text = helper.joins_cleans_text(text)
                if text:
                    if debug:
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
                                               'date': [date]})
                        speeches.append(speech)

                        ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])

                    else:

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

                    if 'Zwischenfrage' in text and current_president:
                        # import pdb; pdb.set_trace()
                        zwischenfrage = True
                        speaker_cnt = 0
                        antwort_s = s
                        antwort_speaker = new_speaker
                        antwort_party = party
                        antwort_executive = executive
                        antwort_servant = servant
                        antwort_president = president
                        antwort_role = role
                    if zwischenfrage:
                        # pdb.set_trace()
                        speaker_cnt += 1

                # stops iterating over lines, if end of session is reached e.g. Schluss: 17:16 Uhr
                if END_MARK.search(line):
                    in_session = False
                    print('reached end of session')
                    break
                # to know order of speech within a given plenary session
                seq += 1
                # resets sub counter (for parts of one speech: speakers' parts and interjections)
                # for next speech
                sub = 0
                # resets current_speaker for next speech
                current_speaker = None
        # adds interjections to the data in such a way that order is maintained
        if INTERJECTION_MARK.match(line) and not interjection:
            # skips lines that start with brackes for abbreviations at the beginning of line e.g. '(EU) Drucksache [...]'
            if NO_INTERJECTION.match(line):
                print('NO INTERJECTION?' + line)
            else:
                # variable contains the number of lines an interjection covers
                interjection_length = 0
                # saves speech of speaker until this very interjection
                if not interjection_complete and current_speaker is not None:
                    text_length = len(text)
                    text = helper.joins_cleans_text(text)
                    text = re.sub('-(?=[a-z])', '', text)
                    if text:
                        if debug:
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
                                                   'date': date})
                            speeches.append(speech)
                            ls_text_length.append([text_length, wp, session, seq, sub, current_speaker, text])


                        else:
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

                #
                sub += 1
                interjection = True
                interjection_text = []
        # special case: interjection
        if interjection:
            # variables contain opening and closing brackets to ensure equality.
            # one measure to determin the end of an interjection
            cnt_brackets_opening += line.count('(')
            cnt_brackets_closing += line.count(')')
            
            # ensures that there is no whitespace at the end of an interjection instead of a closing bracket
            if ')' in line:
                line = line.rstrip()

            # signals end of interjection
            # either line ends with ')' and opening and closing brackets are equal or we had two empty lines in a row
            if cnt_brackets_opening<=cnt_brackets_closing or CHAIR_MARK.match(line):
                # to avoid an error, if interjection is at the beginning without anybod have started speaking
                # was only relevant for bavaria so far.
                if current_speaker is not None:
                    interjection_text.append(line)
                    #interjection_text = [i + ' ' for i in interjection_text if not i.endswith('-')]
                    interjection_text = ''.join(interjection_text)
                    # removes whitespace duplicates
                    interjection_text = re.sub(' +', ' ', interjection_text)
                    # removes whitespaces at the beginning and end
                    interjection_text = interjection_text.strip()
                    interjection_text = re.sub('-(?=[a-z])', '', interjection_text)

                    if debug:

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
                                               'date': date})

                        speeches.append(speech)
                        ls_interjection_length.append([interjection_length, wp, session, seq, sub, current_speaker, interjection_text])

                    else:

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
                                       'date': date}

                        table.insert(speech_dict)

                    sub += 1
                    interjection_length += 1
                interjection = False
                interjection_complete = True
                interjection_skip = False
                cnt_brackets_opening = 0
                cnt_brackets_closing = 0
                continue
            else:
                interjection_text.append(helper.cleans_line(line))
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

                line = helper.cleans_line(line)
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

    if debug:
        pd_session_speeches = pd.concat(speeches)
        ls_speeches.append(pd_session_speeches)

if debug:
    pd_speeches = pd.concat(ls_speeches).reset_index()
    pd_speeches.to_csv('sh_test.csv')

# checks
# interjection length
if debug:
    idx = [i for i, e in enumerate(ls_interjection_length) if e[0] >= 5]

    #text length
    idx_txt = [i for i, e in enumerate(ls_text_length[0:10]) if e[0] > 50]

    pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()