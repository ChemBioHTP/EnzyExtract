from io import StringIO
import json
from lxml import etree

import pandas as pd

with open('data/dev/all_xml_tables.json') as f:
    obj = json.load(f)


# question: how many rowspan/colspan are there?
rowspan = 0
colspan = 0

for k, xmls in obj.items():
    # get the tgroup object from xml
    for xml in xmls:
        xml = etree.fromstring(xml)
        tgroup = xml.find('tgroup')

        # Extract the inner HTML of tgroup
        # tgroup_content = ''.join([etree.tostring(child, encoding='unicode', method='html') for child in tgroup])
        if tgroup is None:
            continue
        tgroup_content = etree.tostring(tgroup, encoding='unicode')
        if 'morerows' in tgroup_content:
            rowspan += 1
        if 'namest' in tgroup_content or 'nameend' in tgroup_content:
            colspan += 1
        # # Wrap tgroup content inside a <table> tag
        # table_html = f"<table>{tgroup_content}</table>"

        # # put into a pandas dataframe
        # df = pd.read_html(StringIO(table_html))
        # print(df)

print(f"Rowspan: {rowspan}")
print(f"Colspan: {colspan}")
print(f"Total: {len(obj)}")
