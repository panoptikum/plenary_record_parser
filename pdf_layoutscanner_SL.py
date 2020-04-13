#!/home/felix/anaconda3/lib/python3.6
import sys
import os
import json
from collections import Counter
from tqdm import tqdm

os.chdir('/Volumes/Datahouse/Users/Stipe/Documents/Studium/Master VWL/Masterarbeit/plenarprotokolle/code')

from lib import layout_collector

def scans_layout_plenary_records(state):

    DATA_PATH = os.environ.get('DATA_PATH', '../data/' + state)

    x0_occurences = []
    x1_occurences = []
    text_boxes = []
    y0_occurences = []
    y1_occurences = []

    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith(".pdf")]

    for filename in tqdm(files):
        #import pdb; pdb.set_trace()
        print(filename)
    # Open a PDF file
        pages, x0_occurences, x1_occurences, text_boxes, y0_occurences, y1_occurences = layout_collector.get_pages(filename, 
        	x0_occurences=x0_occurences, x1_occurences=x1_occurences, 
        	text_boxes=text_boxes,
        	y0_occurences=y0_occurences, y1_occurences=y1_occurences)

    x0_occurences_ls = [item for sublist in x0_occurences for item in sublist]
    x1_occurences_ls = [item for sublist in x1_occurences for item in sublist]
    text_boxes_ls = [item for sublist in text_boxes for item in sublist]
    y0_occurences_ls = [item for sublist in y0_occurences for item in sublist]
    y1_occurences_ls = [item for sublist in y1_occurences for item in sublist]

    c_x0 = Counter(x0_occurences_ls)
    c_x1 = Counter(x1_occurences_ls)
    c_y0 = Counter(y0_occurences_ls)
    c_y1 = Counter(y1_occurences_ls)

    with open(os.path.join(DATA_PATH, "x0_occurences_ls.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(x0_occurences_ls, fp)
    with open(os.path.join(DATA_PATH, "x1_occurences_ls.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(x1_occurences_ls, fp)
    with open(os.path.join(DATA_PATH, "text_boxes_occurences_ls.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(text_boxes_ls, fp)
    with open(os.path.join(DATA_PATH, "y0_occurences_ls.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(y0_occurences_ls, fp)
    with open(os.path.join(DATA_PATH, "y1_occurences_ls.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(y1_occurences_ls, fp)

    with open(os.path.join(DATA_PATH, "c_x0.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(c_x0, fp)
    with open(os.path.join(DATA_PATH, "c_x1.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(c_x1, fp)
    with open(os.path.join(DATA_PATH, "c_y0.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(c_y0, fp)
    with open(os.path.join(DATA_PATH, "c_y1.json"), "w", encoding="utf-8") as fp:   #Pickling
        json.dump(c_y1, fp)

with open(os.path.join(DATA_PATH, "c_x0.json"), encoding="utf-8") as fp:
	c_x0 = json.loads(fp.read())
Counter(c_x0)



# with open(os.path.join(DATA_PATH, "c_y0.json"), encoding="utf-8") as fp:
#     c_y0 = json.loads(fp.read())

# with open(os.path.join(DATA_PATH, "c_y1.json"), encoding="utf-8") as fp:
#     c_y1 = json.loads(fp.read())
# with open(os.path.join(DATA_PATH, "text_boxes_occurences_ls.json"), encoding="utf-8") as fp:
#     text_boxes = json.loads(fp.read())

# with open(os.path.join(DATA_PATH, "x0_occurences_ls.json"), encoding="utf-8") as fp:
#     x0_occurences_ls = json.loads(fp.read())

# with open(os.path.join(DATA_PATH, "y0_occurences_ls.json"), encoding="utf-8") as fp:
#     y0_occurences_ls = json.loads(fp.read())

# with open(os.path.join(DATA_PATH, "y1_occurences_ls.json"), encoding="utf-8") as fp:
#     y1_occurences_ls = json.loads(fp.read())

# # to check on supicious x0 values
# [text_boxes[z] for z in [i for i,x in enumerate(x0_occurences_ls) if x==304]]

# [j for j in [int(i) for i in c_x0.keys()] if (j<311) and (j>250)]

if __name__ == "__main__":
    scans_layout_plenary_records(sys.argv[1])