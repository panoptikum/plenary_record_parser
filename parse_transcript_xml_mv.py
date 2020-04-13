import os
from operator import itemgetter
import re
import sys
import xml.etree.cElementTree as ET
from collections import Counter
import json
import logging

# only one set of pages:
# text x0: 62
# interjection: varies
# text x0: 303
# interjection: varies

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
        interjection = False
        # import pdb; pdb.set_trace()
        left =  [round(float(textbox.attrib["bbox"].split(',')[0:1][0])) for textbox in textboxes]

        line_half = int((params["identation_bound_right_1"] -
                      params["identation_bound_left_1"])/2)

        identation = [e for e in Counter(left).keys()
                      if (e > params["identation_bound_left_1"] + 3
                      and e < params["identation_bound_right_1"] - 3)
                      or
                      (e > params["identation_bound_right_1"] + 3
                      and e < params["identation_bound_right_1"] +
                              line_half)]

        if state not in ['BE', 'BW', 'HB']:
            if any([e in range(params["identation_bound_left_1"], params["identation_bound_left_1"] + line_half) for e in identation]):
                identation_bounds = 'first'
            elif any([e in range(params["identation_bound_left_1"], params["identation_bound_left_1"] + line_half) for e in identation]):
                identation_bounds = 'second'
            else:
                identation_bounds = None

        if not identation:
            logging.warning('no x0 values within specified ranges' + page.attrib['id'])



        # if page_id=='6':
        #     import pdb; pdb.set_trace()
        interjection_left = params['identation_bound_left_2']-3
        interjection_right = params['identation_bound_right_2']-3

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
            issue = False
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
            # elif textbox_bounds[1]<params['footer_bound'] and page_id not in ['1']:
            #     print('removed footer ' + textbox_text)
            #     continue

            # save a description of the line
            textbox = {'left': textbox_bounds[0], 'top': textbox_bounds[1], 'text': textbox_text}

            # if state != 'BE':
            #     if identation_bounds=='first':
            #         if textbox['left']>46 and textbox['left']<290 or textbox['left']>316:
            #             if textbox_text.lstrip().startswith('(') and not NO_INTERJECTION.match(textbox_text):
            #                 textbox['text'] = '<interjection_begin>' + textbox['text'] + '<interjection_end>'
            #             else:
            #                 textbox['text'] = '<identation_begin>' + textbox['text'] + '<identation_end>'
            #     elif identation_bounds=='second':
            #         if textbox['left']>75 and textbox['left']<320 or textbox['left']>344:
            #             if textbox_text.lstrip().startswith('(') and not NO_INTERJECTION.match(textbox_text):
            #                 textbox['text'] = '<interjection_begin>' + textbox['text'] + '<interjection_end>'
            #             else:
            #                 textbox['text'] = '<identation_begin>' + textbox['text'] + '<identation_end>'
            #     else:
            #         logging.info('no ordinary text boxes on page' + page_id)

            # if textbox['left']>params['identation_bound_left_1'] + 3 and textbox['left']<params['identation_bound_right_1'] - 3 or textbox['left']>params['identation_bound_right_2'] + 3:
            #     textbox['text'] = '<identation>' + textbox['text'].replace('\n', '\n<identation>')

            # if 'Schluss:' in textbox_text:
            #     import pdb; pdb.set_trace()
            # if ENDING_MARK.search(textbox_text) and textbox_bounds[0]>250 and textbox_bounds[2]<345:
            #    found_ending_mark = True
            #    textbox['text'] = textbox_text + ' <end>'
            #    textbox['left'] = 30
            #    textbox['top'] = textbox['top']-1000
            if textbox['left'] > interjection_left and textbox['left'] < params['identation_bound_right_1'] - 3:
                textbox['text'] = '<interjection_begin>' + textbox['text'] + '<interjection_end>'
            elif textbox['left'] > interjection_right:
                textbox['text'] = '<interjection_begin>' + textbox['text'] + '<interjection_end>'

            if textbox['left'] < params['identation_bound_right_1'] - 3:
                textbox['left'] = 30
            else:
                textbox['left'] = 30
                textbox['top'] = textbox['top']-1000
            #import pdb; pdb.set_trace()

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
        no_digits = len(filename.split('_')[3].split('.')[0])
        output_name = filename.replace('.xml', '_xml.txt')
        with open(output_name, "w", encoding="utf-8") as fp:
            fp.writelines(result)


if __name__ == "__main__":
    iteratesFiles(sys.argv[1])
