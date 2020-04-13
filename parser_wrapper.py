import sys
import os
from tqdm import tqdm
import re

def converts_pdf_to_text(state, cutoff):

    os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')
    DATA_PATH = os.environ.get('DATA_PATH', '../data/' + state)

    from lib import pdf_parser

    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith(".pdf")]

    if state == 'BY':
        ep_regex = re.compile(r'([0-9]{2})\.\s+Wahlperiode')

    for filename in tqdm(files):
        ep = '0'
        print(filename)
        pages = pdf_parser.get_pages(filename, cutoff=cutoff, y1_max=1000)
        text = ''.join(pages)
        if state == 'BY':
            ep = ep_regex.search(text).group(1)
            print(ep)
            with open(filename.replace('BY/', 'BY/' + ep + '_').replace('.pdf', '.txt'), "w", encoding="utf-8") as fp:
                fp.write(text)
        else:
            with open(filename.replace('.pdf', '.txt'), "w", encoding="utf-8") as fp:
                fp.write(text)            

if __name__ == "__main__":
    converts_pdf_to_text(sys.argv[1], int(sys.argv[2]))