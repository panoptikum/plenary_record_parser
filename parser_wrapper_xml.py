import sys
import os
from tqdm import tqdm


def converts_pdf_to_text(state, word_margin, char_margin, line_margin):

    os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')
    # os.chdir('/home/felix/privat/plenarprotokolle/code')
    DATA_PATH = os.environ.get('DATA_PATH', '../data/' + state)

    from lib import pdf2txt

    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(
        os.path.expanduser(DATA_PATH)) for f in fn if f.endswith('.pdf')]

    for filename in tqdm(sorted(files[:10])):
        os.system("python lib/pdf2txt.py -o{0} {1} -W {2} -M {3} -L {4}".format(
            filename.replace('.pdf', '_new.xml'), filename, word_margin, char_margin, line_margin))


if __name__ == "__main__":
    converts_pdf_to_text(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
