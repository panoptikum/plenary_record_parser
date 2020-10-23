import os
from operator import itemgetter
import re
import sys
import xml.etree.cElementTree as ET
from collections import Counter
import json
import logging

# first set of pages:
# text left x0: 57
# interjection: 71
# text right x0: 312
# interjection: 323

# second set of pages:
# text left x0: 57
# interjection left: 71
# text right x0: 312
# interjection right: 323

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


def parseXML(xml_in, params, state):
    """
    parse the document XML
    """
    # import pdb; pdb.set_trace()
    # if two fragments of text are within LINE_TOLERANCE of each other they're
    # on the same line

    NO_INTERJECTION = re.compile(r'^.{1,3}' + re.escape(params['closing_mark']))

    # ENDING_MARK = re.compile('(\(Schluss der Sitzung:.\d{1,2}.\d{1,2}.Uhr\).*|Schluss der Sitzung)')

    debug = False

    found_ending_mark = False

    # get the page elements
    tree = ET.ElementTree(file=xml_in)
    pages = tree.getroot()

    if pages.tag != "pages":
        sys.exit("ERROR: pages.tag is %s instead of pages!" % pages.tag)

    text = []
    # step through the pages
    for page in pages:
        # gets page_id
        page_id = page.attrib['id']

        # get all the textline elements
        textboxes = page.findall("./textbox")

        #print "found %s textlines" % len(textlines)
        # step through the textlines
        page_text = []
        # if page_id=='4':
        #     import pdb; pdb.set_trace()
        left =  [round(float(textbox.attrib["bbox"].split(',')[0:1][0])) for textbox in textboxes]

        left_margin = [i[0] for i in Counter(left).most_common(4)]

        cnt_set_one = 0
        cnt_set_two = 0

        for e in left_margin:
            if (e in range(params["text_margin_first_left"] - 4, params["text_margin_first_left"] + 4) or
                e in range(params["text_margin_first_right"] - 4, params["text_margin_first_right"] + 4) or
                e in range(params["indentation_bound_first_left"] - 4, params["indentation_bound_first_left"] + 4) or
                e in range(params["indentation_bound_first_right"] - 4, params["indentation_bound_first_right"] + 4)):
                cnt_set_one += 1
            if (e in range(params["text_margin_second_left"] - 4, params["text_margin_second_left"] + 4) or
                e in range(params["text_margin_second_right"] - 4, params["text_margin_second_right"] + 4) or
                e in range(params["indentation_bound_second_left"] - 4, params["indentation_bound_second_left"] + 4) or
                e in range(params["indentation_bound_second_right"] - 4, params["indentation_bound_second_right"] + 4)):
                cnt_set_two += 1

        if cnt_set_one==0 and cnt_set_two==0:
            logging.warning('no x0 values within specified ranges' + page.attrib['id'])
            page_set = None
        else:
            if cnt_set_one > cnt_set_two:
                page_set = 'first'
            else:
                page_set = 'second'

        for textbox in textboxes:
            # get the boundaries of the textline
            #import pdb; pdb.set_trace()
            textbox_bounds = [float(s) for s in textbox.attrib["bbox"].split(',')]
            #print "line_bounds: %s" % line_bounds

            # get all the texts in this textline
            lines = list(textbox)
            #print("found %s characters in this line." % len(chars))

            # combine all the characters into a single string
            textbox_text = ""
            poi = False
            for line, has_more in lookahead(lines):
                chars = list(line)
                for char in chars:
                    if poi: 
                        if char.attrib:
                            if "Bold" not in char.attrib['font']:
                                #import pdb; pdb.set_trace()
                                textbox_text = textbox_text + '<poi_end>'
                                poi = False
                    elif char.attrib:
                        if "Bold" in char.attrib['font']:
                            #import pdb; pdb.set_trace()
                            textbox_text = textbox_text + '<poi_begin>'
                            poi = True
                    textbox_text = textbox_text + char.text
                if not has_more and poi:
                    textbox_text = textbox_text + '<poi_end>'

            textbox_text = textbox_text.replace('\n<poi_end>', '<poi_end>\n').replace('\t', ' ')
            # if 'Beifall' in textbox_text:
            #    import pdb; pdb.set_trace()
            # strip edge & multiple spaces
            textbox_text = re.sub(' +', ' ', textbox_text.strip())

            # removes header/footer
            if textbox_bounds[1]>params['header_bound'] and page_id not in ['1']:
                #import pdb; pdb.set_trace()
                print('removed header ' + textbox_text)
                continue

            # save a description of the line
            textbox = {'left': textbox_bounds[0], 'top': textbox_bounds[1], 'text': textbox_text}

            # if '(Heiterkeit)' in textbox["text"]:
            #     import pdb; pdb.set_trace()
            if page_set=='first':
                if textbox['left'] > params["indentation_bound_first_left"] - 5 and textbox['left'] < params["text_margin_first_right"] - 5:
                    textbox['text'] = '<interjection_begin>' + textbox['text'].replace('\n', '<interjection_end>\n<interjection_begin>') + '<interjection_end>'
                elif textbox['left'] > params["indentation_bound_first_right"] - 5:
                    textbox['text'] = '<interjection_begin>' + textbox['text'].replace('\n', '<interjection_end>\n<interjection_begin>') + '<interjection_end>'

                if textbox['left'] < params['text_margin_first_right'] - 5:
                    textbox['left'] = 30
                else:
                    textbox['left'] = 30
                    textbox['top'] = textbox['top']-1000
            elif page_set=='second':
                if textbox['left'] > params["indentation_bound_second_left"] - 5 and textbox['left'] < params["text_margin_second_right"] - 5:
                    textbox['text'] = '<interjection_begin>' + textbox['text'].replace('\n', '<interjection_end>\n<interjection_begin>') + '<interjection_end>'
                elif textbox['left'] > params["indentation_bound_second_right"] - 5:
                    textbox['text'] = '<interjection_begin>' + textbox['text'].replace('\n', '<interjection_end>\n<interjection_begin>') + '<interjection_end>'

                if textbox['left'] < params['text_margin_second_right'] - 5:
                    textbox['left'] = 30
                else:
                    textbox['left'] = 30
                    textbox['top'] = textbox['top']-1000

            page_text.append(textbox)

        #print "page %s has %s lines" % (page.attrib["id"], len(lines))

        # sort the lines by left, then top position
        # if debug:
        #     import pdb; pdb.set_trace()
        page_text.sort(key=itemgetter('left'))
        page_text.sort(key=itemgetter('top'), reverse=True)

        # consolidate lines that have the same top (within tolerance)
        # consolidated_lines = []
        # line_segments = []
        # line_top = lines[0]['top']
        # for line in lines:
        #   if abs(line['top'] - line_top) < LINE_TOLERANCE:
        #       line_segments.append(line)

        #   else:
        #       # assure that text segments appear in the correct order
        #       line_segments.sort(key=itemgetter('left'))
        #       # create a new line object combining partial texts, preserving the left-most text position
        #       merged_line = dict(line_segments[0])
        #       merged_line['text'] = ""
        #       for item in line_segments:
        #           merged_line['text'] = merged_line['text'] + " " + item['text']

        #       consolidated_lines.append(merged_line)

        #       # reset
        #       line_segments = [line]
        #       line_top = line['top']
        #import pdb; pdb.set_trace()
        page_text = '\n\n'.join([e['text'] for e in page_text])

        text.append(page_text + '\n')

    # if not found_ending_mark:
    #     sys.exit('could not find closing mark; adjust regex')

    #import pdb; pdb.set_trace()
    return text

def iteratesFiles(state):
    DATA_PATH = os.environ.get('DATA_PATH', '../data/' + state)
    files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(DATA_PATH)) for f in fn if f.endswith(".xml")]
    with open(os.path.join(DATA_PATH, "params_" + state + ".json"), encoding="utf-8") as fp:
        params = json.loads(fp.read())
    for filename in sorted(files):
        print(filename)
        result = parseXML(filename, params=params, state=state)
        # no_digits = len(filename.split('_')[3].split('.')[0])
        output_name = filename.replace('.xml', '_xml.txt')
        with open(output_name, "w", encoding="utf-8") as fp:
            fp.writelines(result)


if __name__ == "__main__":
    iteratesFiles(sys.argv[1])
