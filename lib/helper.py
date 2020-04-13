import re
from itertools import tee, islice, zip_longest

def lookahead(iterable):
    """Pass through all values from the given iterable, augmented by the
    information if there are more values to come after the current one
    (True), or if it is the last value (False).
    """
    # Get an iterator and pull the first value.
    it = iter(iterable)
    last = next(it)
    # Run the iterator to exhaustion (starting from the second value).
    for val in it:
        # Report the *previous* value (more to come).
        yield last, True
        last = val
    # Report the last value.
    yield last, False

def get_next(some_iterable, window=2):
    items, nexts = tee(some_iterable, 2)
    nexts = islice(nexts, window, None)
    return zip_longest(items, nexts)

def joins_cleans_text(text):
    """cleans text before it stores to pandas df or sqlite db"""
    # joins list elements that are strings

    text = ''.join(text)
    # removes whitespace duplicates
    text = re.sub(' +', ' ', text)
    # removes whitespaces at the beginning and end
    text = text.strip()
    return text

def cleans_line(line):
    """Cleans line from unwanted characters too many spaces"""
    return re.sub(r'-\s+$', '', line + ' ')

def cleans_line_bb(line):
    """Cleans line from unwanted characters too many spaces"""
    return re.sub(r'-(?:\s+)?$',  ' ', line)

def cleans_line_by(line):
    """Cleans line from unwanted characters too many spaces"""
    line = line + ' '
    line = re.sub(r'-(?:\s+)?$',  '', line)
    return line

def cleans_line_hh(line):
    """Cleans line from unwanted characters too many spaces"""
    return re.sub(r'-(?:\s+)?$',  ' ', line)

def cleans_line_sn(line):
    """Cleans line from unwanted characters too many spaces"""
    return re.sub(r'-(?:\s+)?$',  '', line)

def cleans_executive_speaker_bw(new_speaker, wp, date):
    if wp==15:
        if 'Finanzen und Wirtschaft' in new_speaker:
            new_speaker = 'Nils Schmid'
        elif 'Staatsministerium' in new_speaker:
            new_speaker = 'Silke Krebs'
        elif 'Bundesrat, Europa und internationale' in new_speaker:
            new_speaker = 'Peter Friedrich'
        elif 'Umwelt, Klima und Energiewirtschaft' in new_speaker:
            new_speaker = 'Franz Untersteller'
        elif 'Kultus, Jugend und Sport' in new_speaker:
            if date<='2018-08-01':
                new_speaker = 'Gabriele Warminski-Leitheußer'
            else:
                new_speaker = 'Andreas Stoch'
        elif 'Ländlichen Raum und Verbraucherschutz' in new_speaker:
            new_speaker = 'Alexander Bonde'
        elif 'Wissenschaft, Forschung und Kunst' in new_speaker:
            new_speaker = 'Theresia Bauer'
        elif 'Verkehr und Infrastruktur' in new_speaker:
            new_speaker = 'Winfried Hermann'
        elif 'Arbeit und Sozialordnung, Familie' in new_speaker:
            new_speaker = 'Katrin Altpeter'
        elif 'Integration' in new_speaker:
            new_speaker = 'Bilkay Öney'
    elif wp==16:
        if 'Inneres, Digitalisierung und Migration' in new_speaker:
            new_speaker = 'Thomas Strobl'
        elif 'Finanzen' in new_speaker:
            new_speaker = 'Edith Sitzmann'
        elif 'Kultus, Jugend und Sport' in new_speaker:
            new_speaker = 'Susanne Eisenmann'
        elif 'Wissenschaft, Forschung und Kunst' in new_speaker:
            new_speaker = 'Theresia Bauer'
        elif 'Umwelt, Klima und Energiewirtschaft' in new_speaker:
            new_speaker = 'Franz Untersteller'
        elif 'Wirtschaft, Arbeit und Wohnungsbau' in new_speaker:
            new_speaker = 'Nicole Hoffmeister-Kraut'
        elif 'Soziales und Integration' in new_speaker:
            new_speaker = 'Manfred Lucha'
        elif 'Ländlichen Raum und Verbraucherschutz' in new_speaker:
            new_speaker = 'Peter Hauk'
        elif 'Justiz und für Europa' in new_speaker:
            new_speaker = 'Guido Wolf'
        elif 'Verkehr' in new_speaker:
            new_speaker = 'Winfried Hermann'
    return(new_speaker)

def cleans_speaker_hh(new_speaker):
    """
    removes words that are not part of speaker's name
    """
    new_speaker = (
        re.sub(' +', ' ', new_speaker)
        .replace('Zwischenfrage von ', '')
        .replace('Zwischenbemerkung von ', '')
        .replace(' (fortfahrend)', '')
        .replace(' (unterbrechend)', '')
        .replace('Treuenfels-Frowein', 'Treuenfels')
        .replace('Busch-', 'Buschhütter')
        .replace(' GRÜNE: Im', '')
        .replace('Stapel-', 'Stapelfeldt')
        .replace(' frakti-', '')
        .replace(' DIE', '')
        .replace('Finn Ole', 'Finn-Ole')
        .replace('Nebahat Güclü', 'Nebahat Güçlü')
        .split(':')[0]
        )

    new_speaker = re.sub(r'\s+DIE(?:\s+LIN-)?', '', new_speaker)
    return new_speaker