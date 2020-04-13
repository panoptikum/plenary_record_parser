# plenary record parser 
Parsers for the plenary records of German state parliaments covering two legislative periods between 2008-2018.

# Pipeline

1. Layout scan with files such as pdf_layoutscanner.py to determine where first and second column of pages start
2. PDF2Xml conversion with parser_wrapper_xml for hundreds of files using pdfminer with individual options for each state parliament
3. The files parse_transcript_xml_*.py use the coordinates for each text block, sentence or even letter to concatenate the text order correctly and save it as plain text file. Furthermore, I use charateristics such as boldness, font size, ... to cleary mark speaker's name, interjections and change of speaker's.
4. Plenary_record_parser_xml_*.py parses the custom text file to detect each speech, interjection etc. A speech splits into several rows, if the speaker is interrupted by an interjection or the chair. The pipeline stores the resulting sequence with some meta data in a sqlite data base 
