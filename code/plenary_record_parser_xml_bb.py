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
from datetime import datetime

#os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')
os.chdir('/home/felix/privat/plenarprotokolle/code')

from lib import helper

log = logging.getLogger(__name__)

locale.setlocale(locale.LC_TIME, "de_DE.utf8")

DATA_PATH = os.environ.get('DATA_PATH', '../data/BB')

MINISTERS_WP5 = ['Platzeck', 'Markov', 'Speer', 'Woidke', 'Schöneburg', 'Christoffers', 'Görke',
                 'Baaske', 'Tack', 'Rupprecht', 'Münch', 'Kunst', 'Lieske', 'Vogelsänger', 'Holzschuher']

MINISTERS_WP6 = ['Woidke', 'Görke', 'Schröter', 'Markov', 'Ludwig', 'Golze', 'Karawanskij', 
                 'Gerber', 'Steinbach', 'Baaske', 'Ernst', 'Vogelsänger', 'Schneider', 'Kunst', 'Münch']

# regular expressions to capture speeches of one session
BEGIN_STRING = r'^(?:Beginn\s+der\s+Sitzung|\(Fortsetzung\s+der\s+Sitzung|Beginn\s+der\s+[0-9]{1,2}\.\s+Sitzung)(?::\s+(?:[0-9]{1,2}[.:][0-9]{1,2}|[0-9]{1,2})\s+Uhr|\s+am\s+[0-9]{1,2}\.)'
END_STRING = r'^\(?(?:Ende|Schluss)\s+(?:der\s+)?(?:[0-9]{1,2}\.\s+)?Sitzung(?:\s+am\s+[0-9]{1,2}\.\s+.+?(?:\s+[0-9]{4})?)?:?\s+(?:[0-9]{1,2}[.:][0-9]{1,2}|[0-9]{1,2})\s+Uhr'
CHAIR_STRING = r'^(Alterspräsident(?:in)?|Präsident(?:in)?|Vizepräsident(?:in)?)\s+(.+?):'
SPEAKER_STRING = r'^(.+?)\s+\((.+?)\):'
COMMITTEE_STRING = r'^(.+?)\s+\((Vorsitzender?\s+(?:des|der)\s+(Haupt|Untersuchungs|Petitions)?(?:[Aa]usschusses|Enquetekommission).+)'
EXECUTIVE_STRING_SHORT = r'(?:Ministerpräsident(?:in)?|Minister(?:in)?)\s+(.+?):'
EXECUTIVE_STRING_LONG = r'^(?:Minister(?:in)?)\s+((?:für|der|des\s+Innern)\s+.+)'
EXECUTIVE_STOP_CHARACTERS_STRING = r'[,]'
OFFICIALS_STRING = r'^(Staatssekretär(?:in)?)\s+(?:(.+?):|(im.+))'
LAKD_STRING = r'^(.+?)\s+\(LAkD'
DATA_PROTECTION_STRING = r'^(.+?)\s+\(Landesbeauftragter?\s+für'
LRH_STRING = r'^(?:Herr\s+)?Weiser\s+\(Präsident\s+des\s+Landesrechnungshofes|Präsident\s+des\s+Landesrechnungshofe?s\s+Weiser'
SORBEN_STRING = r'^(.+?)\s+\(Rat\s+für\s+sorbische/wendische Angelegenheiten'

#Ziel (Vorsitzender des Ausschusses für Haushaltskontrolle): *

#Herr  Weiser  (Präsident  des  Landesrechnungshofes  Bran-
#denburg):
# compilation of regular expressions
# advantage combination of strings is possible
BEGIN_MARK = re.compile(BEGIN_STRING)
END_MARK = re.compile(END_STRING)
CHAIR_MARK = re.compile(CHAIR_STRING)
SPEAKER_MARK = re.compile(SPEAKER_STRING)
COMMITTEE_MARK = re.compile(COMMITTEE_STRING)
EXECUTIVE_MARK_SHORT = re.compile(EXECUTIVE_STRING_SHORT)
EXECUTIVE_MARK_LONG = re.compile(EXECUTIVE_STRING_LONG)
EXECUTIVE_STOP_CHARACTERS = re.compile(EXECUTIVE_STOP_CHARACTERS_STRING)
OFFICIALS_MARK = re.compile(OFFICIALS_STRING)
LRH_MARK = re.compile(LRH_STRING)
LAKD_MARK = re.compile(LAKD_STRING)
DATA_PROTECTION_MARK = re.compile(DATA_PROTECTION_STRING)
SORBEN_MARK = re.compile(SORBEN_STRING)
#SPEECH_ENDS = re.compile("|".join([CHAIR_STRING, SPEAKER_STRING, EXECUTIVE_STRING, OFFICIALS_STRING]))
INTERJECTION_MARK = re.compile(r'^\(')
INTERJECTION_END = re.compile(r'\)')
NO_INTERJECTION = re.compile(r'\){2,10}')
HEADER_MARK = re.compile(r'^Landtag\s+Brandenburg\s+-\s+[0-9](?:\s+|\.)\s+Wahlperiode')
HEADER_SPEAKER_MARK = re.compile(r'\((?:Abg\.)|\(Alterspräsident(?:in)?|\(Präsident(?:in)?|\(Vizepräsident(?:in)?|\(Staatssekretär(?:in)?|\(Minister(?:in)?|\(Justizminister(?:in)?:|\(Finanzminister(?:in):|\(Innenminister(?:in)?|\(Ministerpräsident(?:in)?')
DATE_CAPTURE = re.compile(r'([0-9]{1,2}\.\s+(?:.+?)\s+[0-9]{4})')

STATE = 'BB'
ls_speeches = []
files = sorted([os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith(".txt")])

db = os.environ.get('DATABASE_URI', 'sqlite:///../data/data.sqlite')
eng = dataset.connect(db)
table = eng['de_landesparlamente_plpr']

#
def strips_line_down_2_speaker(new_speaker, wp, date):
    if wp==5:
        new_speaker = re.sub(r'(?:Prof\.\s+)?(?:Dr\.(?:\s+)?)?(?:-Ing\.\s+Dr\.\s+)?', '', new_speaker)
        new_speaker = re.sub(r'für\s+Wirtschaft\s+und Europaangelegenheiten(?:\s+)?(?:Chris-)?', 'Christoffers', new_speaker)
        new_speaker = re.sub(r'(?:im\s+Ministerium\s+)?(?:der|des)\s+(?:Finanzen|Innern|Justiz)\s+', '', new_speaker)
        new_speaker = re.sub(r'für\s+Arbeit,\s+Soziales,\s+Frauen\s+und\s+Familie.+', 'Baaske', new_speaker)
        new_speaker = re.sub(r'für\s+Umwelt,\s+Gesundheit\s+und\s+Verbraucher(?:schutz|-)(?:.+)?', 'Tack', new_speaker)
        if date<'2011-01-28':
            new_speaker = re.sub(r'für\s+Bildung,\s+Jugend\s+und\s+Sport.+', 'Rupprecht', new_speaker)
        else:
            new_speaker = re.sub(r'für\s+(?:Bildung,\s+Jugend|Jugend,\s+Bildung)\s+und\s+Sport.+', 'Münch', new_speaker)
        if date<'2010-02-25':
            new_speaker = re.sub(r'für\s+Infrastruktur\s+und\s+Landwirtschaft.+', 'Lieske', new_speaker)
        else:
            new_speaker = re.sub(r'für\s+Infrastruktur\s+und\s+Landwirtschaft.+', 'Vogelsänger', new_speaker)
        if date<'2011-02-23':
            new_speaker = re.sub(r'für\s+Wissenschaft,\s+Forschung\s+und\s+Kultur(?:.+)?', 'Münch', new_speaker)
        else:
            new_speaker = re.sub(r'für\s+Wissenschaft,\s+Forschung\s+und\s+Kultur(?:.+)?', 'Kunst', new_speaker)
        # staatssekretäre
        new_speaker = re.sub(r'im\s+Ministerium\s+für\s+Arbeits?,\s+Soziales,\s+Frau(?:en|-)', 'Schroeder', new_speaker)
        new_speaker = re.sub(r'im\s+Ministerium\s+für\s+Bildung,\s+Jugend\s+und', 'Jungkamp', new_speaker)
        new_speaker = re.sub(r'im\s+Ministerium\s+für\s+Infrastruktur\s+und(?:\s+Land-)?', 'Schneider', new_speaker)
        new_speaker = re.sub(r'im\s+Ministerium\s+für\s+Umwelt,\s+Gesundheit\s+und', 'Rühmkorf', new_speaker)
        new_speaker = re.sub(r'im\s+Ministerium\s+für\s+Wirtschaft\s+und(?:\s+Euro-)?', 'Heidemanns', new_speaker)
        new_speaker = re.sub(r'im\s+Ministerium\s+für\s+Wissenschaft,\s+For(?:-|schung)', 'Gorholt', new_speaker)

        new_speaker = re.sub(r'(?:Vogel-|Vogelsän-)', 'Vogelsänger', new_speaker)
        new_speaker = re.sub(r'Dellmann?', 'Dellmann', new_speaker)
        new_speaker = new_speaker.replace('Frau ', '').replace('Herr ', '').replace('*', '').replace(':', '').strip()
        if new_speaker=='Schulz':
            #import pdb; pdb.set_trace()
            new_speaker = 'Schulz-Höpfner'
    elif wp==6:
        new_speaker = re.sub(r'(?:Prof\.\s+)?(?:Dr\s?\.(?:\s+)?)?(?:-Ing\.\s+(?:Dr\.\s+)?)?', '', new_speaker)
        new_speaker = re.sub(r'für\s+Wirtschaft\s+und\s+Energie\s+Stein-?', 'Steinbach', new_speaker)
        new_speaker = re.sub(r'für\s+Wirtschaft\s+und\s+Energie\s+Gerber', 'Gerber', new_speaker)
        new_speaker = re.sub(r'für\s+Infrastruktur\s+und\s+Landesplanung\s+Schnei(?:-|der)\s?', 'Schneider', new_speaker)
        new_speaker = re.sub(r'des\s+Innern\s+und(?:\s+für)?\s+Komm?unales(?:\s+Schröter)?', 'Schröter', new_speaker) 
        new_speaker = re.sub(r'für\s+Inneres\s+und(?:\s+für)?\s+Komm?unales(?:\s+Schröter)?', 'Schröter', new_speaker) 
        new_speaker = re.sub(r'(?:im\s+Ministerium\s+)?(?:der)\s+(?:Finanzen)', '', new_speaker) # Görke
        new_speaker = re.sub(r'für\s+[lL]ändliche\s+Entwicklung,\s+Umwelt\s+und\s+Land(?:-|wirt-)', 'Vogelsänger', new_speaker)
        if date < '2017-09-28':
            new_speaker = re.sub(r'für\s+Bildung,\s+Jugend\s+und\s+Sport.+', 'Baaske', new_speaker)
        else:
            new_speaker = re.sub(r'für\s+Bildung,\s+Jugend\s+und\s+Sport.+', 'Ernst', new_speaker)
        if date < '2016-04-23':
            new_speaker = re.sub(r'der Justiz\s+und\s+für\s+Europa\s+und\s+Verbraucherschutz\s?', 'Markov', new_speaker)
        else:
            new_speaker = re.sub(r'der Justiz\s+und\s+für\s+Europa\s+und\s+Verbraucherschutz\s?', 'Ludwig', new_speaker)
        if date < '2016-03-09':
            new_speaker = re.sub(r'für\s+Wissenschaft,\s+Forschung\s+und\s+Kultur(?:\s+.+)?', 'Kunst', new_speaker)
        else:
            new_speaker = re.sub(r'für\s+Wissenschaft,\s+Forschung\s+und\s+Kultur(?:\s+.+)?', 'Münch', new_speaker)
        if date < '2018-08-28':
            new_speaker = re.sub(r'für\s+Arbeit,\s+Soziales,\s+Gesundheit,\s+Frauen.+', 'Golze', new_speaker)
        elif date >= '2018-08-28' and date < '2018-09-19':
            new_speaker = re.sub(r'für\s+Arbeit,\s+Soziales,\s+Gesundheit,\s+Frauen.+', 'Ludwig', new_speaker)
        else:
            new_speaker = re.sub(r'für\s+Arbeit,\s+Soziales,\s+Gesundheit,\s+Frauen.+', 'Karawanskij', new_speaker)
        # Staatssekretärinnen
        new_speaker = new_speaker.replace('im Ministerium der Justiz und für Europa', 'Pienky')
        new_speaker = new_speaker.replace('im Ministerium des Innern und für Kom-', 'Lange')
        new_speaker = new_speaker.replace('im Ministerium für Arbeit, Soziales, Ge-', 'Hartwig-Tiedt')
        new_speaker = new_speaker.replace('im Ministerium für Bildung, Jugend und' ,'Drescher')
        new_speaker = new_speaker.replace('im Ministerium für Infrastruktur und', 'Lange')
        new_speaker = new_speaker.replace('im Ministerium für Ländliche Entwick-', 'Schilde')
        new_speaker = new_speaker.replace('im Ministerium für Wissenschaft, For-', 'Gutheil')
        # mp 
        new_speaker = new_speaker.replace('Dombrowksi', 'Dombrowski').replace('Redman', 'Redmann')
        new_speaker = new_speaker.replace('Frau ', '').replace('Herr ', '').replace(':', '').replace('*', '').strip()
    return(new_speaker)

def cleans_party_names(party):
    party = re.sub(r'BÜNDNIS\s+90/DIE\s+GRÜNEN|GRÜNE/B?90|B90/G(?:RÜNE|rüne|R0ÜNE)', 'GRÜNE', party)
    # wp6
    party = re.sub(r'BVB(?:/|\s)FREIE\s+WÄHLER\s+GRUPPE', 'BVB/FREIE WÄHLER', party)
    party = party.replace('FPD', 'FDP').replace('Die LINKE', 'DIE LINKE').replace('fraktionlos', 'fraktionslos')
    return(party)

ls_interjection_length = []
ls_text_length = []

# debug mode
debug = True

for filename in files:

    wp, session, date = int(filename[23:24]), int(filename[25:28]), None
    print(wp, session)

    with open(filename, 'rb') as fh:
        text = fh.read().decode('utf-8')

    if wp==5:
        MINISTERS=MINISTERS_WP5
    elif wp==6:
        MINISTERS=MINISTERS_WP6
    else:
        print('error: no minister list for this election period')

    print("Loading transcript: %s/%.3d, from %s" % (wp, session, filename))

    # deletes existing entries for this state's election period and session. e.g SH 18, 001
    if not debug:
        table.delete(wp=wp, session=session, state=STATE)

    #import pdb; pdb.set_trace()
    lines = text.split('\n')

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

    # # trigger to find parts where zwischenfragen are continued without labelling current speaker
    # zwischenfrage = False
    # speaker_cnt = 0

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

    # import pdb; pdb.set_trace()
    for line, has_more in helper.lookahead(lines):
        # if line=='Ministerpräsident Dr. Woidke: ':
        #     import pdb; pdb.set_trace()
        # if in_session:
        #     import pdb; pdb.set_trace()
        # if line=='Minister  Vogelsänger  genau  das  Gegenteil  erklärt:  Wir  sind':
        #     import pdb; pdb.set_trace()
        if not date_captured and DATE_CAPTURE.search(line):
            date = DATE_CAPTURE.search(line).group(1)
            date = datetime.strptime(date, '%d. %B %Y').strftime('%Y-%m-%d')
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
        if HEADER_MARK.search(line) or line.isdigit():
           continue
        if not INTERJECTION_MARK.match(line) and not interjection:
            if CHAIR_MARK.match(line) and not LRH_MARK.match(line):
            #import pdb; pdb.set_trace()
                s = CHAIR_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(2))
                president = True
                executive = False
                servant = False
                party = None
                role = 'chair'
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date).replace('Frisch', 'Fritsch')
            elif EXECUTIVE_MARK_LONG.match(line): # and not EXECUTIVE_STOP_CHARACTERS.search(line):
                    # import pdb; pdb.set_trace()
                s = EXECUTIVE_MARK_LONG.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                role = 'executive'
                party = None
                president = False
                executive = True
                servant = False
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date)
            elif EXECUTIVE_MARK_SHORT.match(line):
                if any([e in MINISTERS for e in EXECUTIVE_MARK_SHORT.match(line).group(1).split(' ')]):
                    s = EXECUTIVE_MARK_SHORT.match(line)
                    new_speaker = re.sub(' +', ' ', s.group(1))
                    new_speaker = re.sub(r'(?:Prof\.\s+)?(?:Dr\s?\.(?:\s+)?)?(?:-Ing\.\s+(?:Dr\.\s+)?)?', '', new_speaker)
                    role = 'executive'
                    party = None
                    president = False
                    executive = True
                    servant = False
 
            elif OFFICIALS_MARK.match(line):
                s = OFFICIALS_MARK.match(line)
                if s.group(2):
                    new_speaker = re.sub(' +', ' ', s.group(2))
                elif s.group(3):
                    new_speaker = re.sub(' +', ' ', s.group(3))
                party = None
                president = False
                executive = False
                servant = True
                role = 'commissioner'
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date)
            elif COMMITTEE_MARK.match(line):
                s = COMMITTEE_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = None
                role = 'committee chairperson'
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date)
            elif LRH_MARK.match(line):
                s = LRH_MARK.match(line)
                new_speaker = 'Weiser'
                president = False
                executive = False
                servant = False
                party = None
                role = 'court of audit'
            elif LAKD_MARK.match(line):
                s = LAKD_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = None
                role = 'LAkD'
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date)
            elif DATA_PROTECTION_MARK.match(line):
                s = DATA_PROTECTION_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = None
                role = 'data protection officer'
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date)
            elif SORBEN_MARK.match(line):
                s = SORBEN_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = None
                role = 'council for sorbian affairs'
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date)
            elif SPEAKER_MARK.match(line):
                s = SPEAKER_MARK.match(line)
                new_speaker = re.sub(' +', ' ', s.group(1))
                president = False
                executive = False
                servant = False
                party = cleans_party_names(s.group(2))
                if party == 'Vorsitzender des Wahlprüfungsausschusses':
                    role = party
                    party = None
                role = 'mp'
                new_speaker = strips_line_down_2_speaker(new_speaker, wp, date)
        # if new_speaker=='-Ing. Steinbach':
        #     import pdb; pdb.set_trace()
        if s is not None and current_speaker is not None:
            #import pdb; pdb.set_trace()
            if new_speaker!=current_speaker or END_MARK.search(line) or not has_more:
                text_length = len(text)
                # concatenates lines to one string
                text = [i for i  in text if not i.isspace()]
                #import pdb; pdb.set_trace()
                text = ''.join(text)
                # removes whitespace duplicates
                text = re.sub(' +', ' ', text)
                # removes whitespaces at the beginning and end
                text = text.strip()
                text = re.sub('-(?=[a-z])', '', text)
                text = re.sub(r'\s+\.', '.', text)
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
                if END_MARK.search(line):
                    in_session = False
                    break
                seq += 1
                sub = 0
                current_speaker = None
        # adds interjections to the data in such a way that order is maintained
        if INTERJECTION_MARK.match(line) and not interjection:
            # skips lines that simply refer to the current speaker
            # if HEADER_SPEAKER_MARK.match(line):
            #     continue
            #text = [i + ' ' if not i.endswith('-') else i for i in text]
             # concatenates lines to one string
            if NO_INTERJECTION.match(line):
                print('NO INTERJECTION? ' + line)
            else:
                interjection_length = 0 
                if not interjection_complete and current_speaker is not None:
                    #import pdb; pdb.set_trace()
                    text_length = len(text)
                    text = [i for i in text if not i.isspace()]
                    text = ''.join(text)
                    # removes whitespace duplicates
                    text = re.sub(' +', ' ', text)
                    # removes whitespaces at the beginning and end
                    text = text.strip()
                    text = re.sub('-(?=[a-z])', '', text)
                    text = re.sub(r'\s+\.', '.', text)
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
                #import pdb; pdb.set_trace()
                sub += 1
                interjection = True
                interjection_text = []
        if interjection:
            cnt_brackets_opening += line.count('(')
            cnt_brackets_closing += line.count(')')
            #import pdb; pdb.set_trace()
            if INTERJECTION_END.search(line) and cnt_brackets_opening<=cnt_brackets_closing or CHAIR_MARK.match(line):
                if current_speaker is not None:
                    interjection_text.append(helper.cleans_line(line))
                    interjection_text = [i + ' ' if not i.endswith('-') else i for i in interjection_text]
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
            line = helper.cleans_line(line)
            # else:
            #     line = ''
            #     dotNotOnFirstLine = True
            text = []
            text.append(line)
            current_speaker = new_speaker
            current_party = party
            current_president = president
            current_executive = executive
            current_servant = servant
            current_role = role

        if not has_more and (in_session or interjection):
            print(str(wp) + ' ' + str(session) + ' : no match for end mark -> error')

    if debug:
        pd_session_speeches = pd.concat(speeches)
        ls_speeches.append(pd_session_speeches)

if debug:
    pd_speeches = pd.concat(ls_speeches).reset_index()
    pd_speeches.to_csv(os.path.join(DATA_PATH, STATE + '_test.csv'))



# checks
# interjection length
if debug:
    idx = [i for i, e in enumerate(ls_interjection_length) if e[0] >= 5]

    #text length
    idx_txt = [i for i, e in enumerate(ls_text_length[0:10]) if e[0] > 50]

    pd_speeches.loc[:, ['wp', 'session', 'seq']].groupby(['wp', 'session']).max()
    pd_speeches.drop_duplicates(['speaker', 'session', 'seq', 'wp']).groupby(['speaker'])['speech'].count().sort_values()