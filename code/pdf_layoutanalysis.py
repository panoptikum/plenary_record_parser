import sys
import os
import json
from collections import Counter
from tqdm import tqdm

with open(os.path.join(DATA_PATH, "c_x0.json"), encoding="utf-8") as fp:
	c_x0 = json.loads(fp.read())
Counter(c_x0)



with open(os.path.join(DATA_PATH, "c_y0.json"), encoding="utf-8") as fp:
    c_y0 = json.loads(fp.read())

with open(os.path.join(DATA_PATH, "c_y1.json"), encoding="utf-8") as fp:
    c_y1 = json.loads(fp.read())
with open(os.path.join(DATA_PATH, "text_boxes_occurences_ls.json"), encoding="utf-8") as fp:
    text_boxes = json.loads(fp.read())

with open(os.path.join(DATA_PATH, "x0_occurences_ls.json"), encoding="utf-8") as fp:
    x0_occurences_ls = json.loads(fp.read())

with open(os.path.join(DATA_PATH, "y0_occurences_ls.json"), encoding="utf-8") as fp:
    y0_occurences_ls = json.loads(fp.read())

with open(os.path.join(DATA_PATH, "y1_occurences_ls.json"), encoding="utf-8") as fp:
    y1_occurences_ls = json.loads(fp.read())

# to check on supicious x0 values
[text_boxes[z] for z in [i for i,x in enumerate(x0_occurences_ls) if x==304]]

[j for j in [int(i) for i in c_x0.keys()] if (j<311) and (j>250)]