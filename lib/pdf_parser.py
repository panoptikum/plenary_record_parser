#!/home/felix/anaconda3/lib/python3.6

import sys
import os
from binascii import b2a_hex
import numpy as np

###
### pdf-miner requirements
###

from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument, PDFNoOutlines
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBox, LTTextLine, LTFigure, LTImage, LTChar

def with_pdf (pdf_doc, fn, cutoff, y1_max, pdf_pwd, *args):
    """Open the pdf document, and apply the function, returning the results"""
    result = None
    try:
        # open the pdf file
        fp = open(pdf_doc, 'rb')
        # create a parser object associated with the file object
        parser = PDFParser(fp)
        # create a PDFDocument object that stores the document structure
        doc = PDFDocument(parser)
        # connect the parser and document objects
        parser.set_document(doc)
        # supply the password for initialization
        #doc.initialize(pdf_pwd)

        if doc.is_extractable:
            # apply the function and return the result
            result = fn(doc, cutoff, y1_max, *args)

        # close the pdf file
        fp.close()
    except IOError:
        # the file doesn't exist or similar problem
        pass
    return result

###
### Extracting Images
###

def write_file (folder, filename, filedata, flags='w'):
    """Write the file data to the folder and filename combination
    (flags: 'w' for write text, 'wb' for write binary, use 'a' instead of 'w' for append)"""
    result = False
    if os.path.isdir(folder):
        try:
            file_obj = open(os.path.join(folder, filename), flags)
            file_obj.write(filedata)
            file_obj.close()
            result = True
        except IOError:
            pass
    return result

def determine_image_type (stream_first_4_bytes):
    """Find out the image file type based on the magic number comparison of the first 4 (or 2) bytes"""
    file_type = None
    bytes_as_hex = b2a_hex(stream_first_4_bytes)
    if bytes_as_hex.startswith(b'ffd8'):
        file_type = '.jpeg'
    elif bytes_as_hex == '89504e47':
        file_type = '.png'
    elif bytes_as_hex == '47494638':
        file_type = '.gif'
    elif bytes_as_hex.startswith(b'424d'):
        file_type = '.bmp'
    return file_type

def save_image (lt_image, page_number, images_folder):
    """Try to save the image data from this LTImage object, and return the file name, if successful"""
    result = None
    if lt_image.stream:
        file_stream = lt_image.stream.get_rawdata()
        if file_stream:
            file_ext = determine_image_type(file_stream[0:4])
            if file_ext:
                file_name = ''.join([str(page_number), '_', lt_image.name, file_ext])
                if write_file(images_folder, file_name, file_stream, flags='wb'):
                    result = file_name
    return result


###
### Extracting Text
###

def to_bytestring (s, enc='utf-8'):
    """Convert the given unicode string to a bytestring, using the standard encoding,
    unless it's already a bytestring"""
    if s:
        if isinstance(s, str):
            return s
        else:
            return s.encode(enc)

def update_page_text_hash (h, lt_obj, cutoff, y1_max):
    """Use the bbox x0,x1 values within pct% to produce lists of associated text within the hash"""

    x0 = lt_obj.bbox[0]
    x1 = lt_obj.bbox[2]
    y0 = lt_obj.bbox[1]
    y1 = lt_obj.bbox[3]

    #import pdb; pdb.set_trace()
    # key_found = False
    # for k, v in h.items():
    #     hash_x0, hash_x1 = k[0], k[1]
    #     if hash_x0 == 57 and hash_x1 == 290:
    #         key_found = True
    #         v.append(to_bytestring(lt_obj.get_text()))
    #         h[k] = v
    #     elif hash_x0 == 305 and hash_x1 == 540:
    #         key_found = True
    #         v.append(to_bytestring(lt_obj.get_text()))
    #         h[k] = v

    # if not key_found:
    if x0<cutoff:
        h[(y1, 30)] = [to_bytestring(lt_obj.get_text())]
    else:
        h[(y1-y1_max, 300)] = [to_bytestring(lt_obj.get_text())]
    #import pdb; pdb
    return h


def parse_lt_objs (lt_objs, page_number, cutoff, y1_max, images_folder, text_content=None):
    """Iterate through the list of LT* objects and capture the text or image data contained in each"""
    if text_content is None:
        text_content = []


    page_text = {} # k=(x0, x1) of the bbox, v=list of text strings within that bbox width (physical column)
    for lt_obj in lt_objs:
        if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
            # determines left and right margin

            # text, so arrange is logically based on its column width
            page_text = update_page_text_hash(page_text, lt_obj, cutoff, y1_max)
        elif isinstance(lt_obj, LTImage):
            # an image, so save it to the designated folder, and note its place in the text
            # saved_file = save_image(lt_obj, page_number, images_folder)
            # if saved_file:
            #     # use html style <img /> tag to mark the position of the image within the text
            #     text_content.append('<img src="'+os.path.join(images_folder, saved_file)+'" />')
            # else:
            #     print >> sys.stderr, "error saving image on page", page_number, lt_obj.__repr__
            print('image detected, not saved')
        elif isinstance(lt_obj, LTFigure):
            # LTFigure objects are containers for other LT* objects, so recurse through the children
            text_content.append(parse_lt_objs(lt_obj, page_number, cutoff, y1_max, images_folder, text_content))
    
    #import pdb; pdb.set_trace()
    for k, v in sorted([(key,value) for (key,value) in page_text.items()], key=lambda x:(x[0][0],-x[0][1]), reverse=True):
        # sort the page_text hash by the keys (x0,x1 values of the bbox),
        # which produces a top-down, left-to-right sequence of related columns
        text_content.append(''.join(v))

    #import pdb; pdb.set_trace()
    return '\n'.join(text_content)

###
### Processing Pages
###

def _parse_pages (doc, cutoff, y1_max, images_folder):
    """With an open PDFDocument object, get the pages and parse each one
    [this is a higher-order function to be passed to with_pdf()]"""
    rsrcmgr = PDFResourceManager()
    laparams = LAParams(boxes_flow=0.3, char_margin=4)
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)

    text_content = []

    for i, page in enumerate(PDFPage.create_pages(doc)):
        interpreter.process_page(page)
        # receive the LTPage object for this page
        layout = device.get_result()
        # layout is an LTPage object which may contain child objects like LTTextBox, LTFigure, LTImage, etc.
        text_content.append(parse_lt_objs(layout, (i+1), cutoff, y1_max, images_folder))

    return text_content

def get_pages (pdf_doc, cutoff, y1_max, pdf_pwd='', images_folder='/tmp'):
    """Process each of the pages in this pdf file and return a list of strings representing the text found in each page"""
    return with_pdf(pdf_doc, _parse_pages, cutoff, y1_max, pdf_pwd, *tuple([images_folder]))