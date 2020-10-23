import re
import pandas as pd

def stores_speech_metadata(text, speeches, current):
    """
    joins list of strings and returns a pandas
    dataframe with the meta data of the text
    """
    # concatenates lines to one string
    # text = [i + ' ' if not i.endswith('-') else i.replace('-', '') for i in text]
    text = ''.join(text)
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
        return speeches


def finds_party(new_speaker, party, date, wp, line, dict_speaker):

    if wp == 20:
        if 'Philipp-Sebastian' in line:
            party = 'SPD'
            new_speaker = 'Philipp-Sebastian Kühn'
        elif 'Ole Thorben Buschhüter' in line:
            party = 'SPD'
        elif 'Dr. Thomas-Sönke Kluth' in line:
            party = 'FDP'
        elif 'Kai Voet van Vormizeele' in line:
            party = 'CDU'
        elif 'Dr. Wieand Schinkenburg' in line:
            new_speaker = 'Dr. Wieland Schinnenburg'
            party = 'FDP'
        elif 'Andrea Rugbarth' in line:
            party = 'SPD'
        elif 'Dr. Walter Scheuerl' in line:
            if date > '2014-03-24':
                party = 'fraktionslos'
            else:
                party = 'CDU'
            new_speaker = 'Dr. Walter Scheuerl'
        elif 'Ekkehard Wysocki' in line:
            party = 'SPD'
        elif 'Christiane Schneider' in line:
            new_speaker = 'Christiane Schneider'
            party = 'DIE LINKE'
        elif 'Heike Sudmann'in line:
            new_speaker = 'Heike Sudmann'
            party = 'DIE LINKE'
        elif 'Juliane Timmermann' in line:
            party = 'SPD'
        elif 'Finn Ole Ritter' in line:
            new_speaker = 'Finn-Ole Ritter'
        else:
            if new_speaker in dict_speaker.keys():
                party = dict_speaker[new_speaker]
    elif wp == 21:
        if 'Karl-Heinz Warnholz' in line:
            party = 'CDU'
        elif 'Dr. Wieland Schinnenburg' in line:
            party = 'FDP'
        elif 'Martin Dolzer' in line:
            new_speaker = 'Martin Dolzer'
            party = 'DIE LINKE'
        elif 'Christiane Schneider' in line:
            new_speaker = 'Christiane Schneider'
            party = 'DIE LINKE'
        elif 'Ole Thorben Busch' in line:
            new_speaker = 'Ole Thorben Buschhüter'
            party = 'SPD'
        elif 'Heike Sudmann' in line:
            new_speaker = 'Heike Sudmann'
            party = 'DIE LINKE'
        elif 'Dorothee Martin' in line:
            party = 'SPD'
        elif 'Anna-Elisabeth von Treuen' in line:
            new_speaker = 'Anna-Elisabeth von Treuenfels'
            party = 'FDP'
        elif 'Inge Hannemann' in line:
            new_speaker = 'Inge Hannemann'
            party = 'DIE LINKE'
        elif 'Dietrich Wersich' in line:
            party = 'CDU'
        elif 'Dr. Andreas Dressel' in line:
            party = 'SPD'
        elif 'Norbert Hackbusch' in line:
            new_speaker = 'Norbert Hackbusch'
            party = 'DIE LINKE'
        elif 'Phyliss Demirel' in line:
            new_speaker = 'Phyliss Demirel'
            party = 'GRÜNE'
        elif 'Dr. Mathias Petersen' in line:
            party = 'SPD'
        elif 'Deniz Celik' in line:
            new_speaker = 'Deniz Celik'
            party = 'DIE LINKE'
        else:
            if new_speaker in dict_speaker.keys():
                party = dict_speaker[new_speaker]

    new_speaker = new_speaker.replace('Zwischenbemerkung von ', '')

    return new_speaker, party