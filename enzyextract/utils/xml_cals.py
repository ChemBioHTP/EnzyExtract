"""
convert CALS Table Model to html
"""

from lxml import etree

# Function to remove namespaces
def strip_namespaces(element):
    # Remove namespace from the current element
    element.tag = etree.QName(element).localname
    # Recursively remove namespaces from child elements
    for child in element:
        strip_namespaces(child)

# Helper function to get full text including children
def get_full_text(element):
    text = (element.text or '')  # Start with direct text of the element, if any
    for child in element:         # Loop over children
        text += etree.tostring(child, encoding='unicode', method='text')  # Add child text
        if child.tail:
            text += child.tail     # Add tail text after child
    return text.strip()            # Strip to remove extra whitespace

def get_col_num(colname: str):
    prefix = 0
    if colname.startswith("col"):
        # get the numeric part
        numeric = ''.join([c for c in colname if c.isdigit()])
        return int(numeric)
    elif colname.startswith("c"):
        prefix = 1
    return int(colname[prefix:])

def parse_cals_table(xml_str):
    # delete some stuff
    # xml_str = xml_str.replace('xmlns:ce="http://www.elsevier.com/xml/common/dtd"', '')
    # xml_str = xml_str.replace('xmlns:xoe="http://www.elsevier.com/xml/xoe/dtd"', '')
    # xml_str = xml_str.replace('xmlns:bk="http://www.elsevier.com/xml/bk/dtd"', '')
    # xml_str = xml_str.replace('xmlns:cals="http://www.elsevier.com/xml/common/cals/dtd"', '')
    # xml_str = xml_str.replace('xmlns:ja="http://www.elsevier.com/xml/ja/dtd"', '')
    # xml_str = xml_str.replace('xmlns:mml="http://www.w3.org/1998/Math/MathML"', '')
    # xml_str = xml_str.replace('xmlns:sa="http://www.elsevier.com/xml/common/struct-aff/dtd"', '')
    # xml_str = xml_str.replace('xmlns:sb="http://www.elsevier.com/xml/common/struct-bib/dtd"', '')
    # xml_str = xml_str.replace('xmlns:tb="http://www.elsevier.com/xml/common/table/dtd"', '')
    # xml_str = xml_str.replace('xmlns:xlink="http://www.w3.org/1999/xlink"', '')
    # xml_str = xml_str.replace('xmlns:xocs="http://www.elsevier.com/xml/xocs/dtd"', '')
    # xml_str = xml_str.replace('xmlns:dc="http://purl.org/dc/elements/1.1/"', '')
    # xml_str = xml_str.replace('xmlns:dcterms="http://purl.org/dc/terms/"', '')
    # xml_str = xml_str.replace('xmlns:prism="http://prismstandard.org/namespaces/basic/2.0/"', '')
    # xml_str = xml_str.replace('xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"', '')

    namespaces = {
        'ce': 'http://www.elsevier.com/xml/common/dtd',
        'cals': 'http://www.elsevier.com/xml/common/cals/dtd'
    }

    # Parse the XML structure
    root = etree.fromstring(xml_str)

    strip_namespaces(root)

    # Start building HTML content
    html_table = "<table>\n"

    label_content = ''
    # caption_content = ''
    # # Handle table label and caption if they exist
    label = root.find(".//label")
    if label is not None:
        label_content = label.text + '\n'

    # caption = root.find(".//ce:caption", namespaces=namespaces)
    # if caption is not None:
    #     caption_content = ''.join(caption.itertext()).strip()

    # take anything not in table (ie. label, caption, table-footnote) and concat the text
    for child in root.iterchildren():
        # if etree.QName(child).localname not in ['tgroup', 'thead', 'tbody', 'label']:
        if child.tag not in ['tgroup', 'thead', 'tbody', 'label']:
            label_content += ''.join(child.itertext()).strip() + '\n'

    # tgroups = root.xpath('.//tgroup') 
    tgroups = root.xpath(".//*[local-name()='tgroup']")

    # i hate namespaces
    for child in root.iterchildren():
        if 'tgroup' in child.tag and child.tag != 'tgroup':
            print(f"Tag: {child.tag}, Text: {child.text}")
            
        # return None, None, None
    
    if not tgroups:
        # this can legitamately happen
        return None, None


    # Handle table headers
    tgroup = tgroups[0]




    # Remove namespaces from the `tgroup` element
    # strip_namespaces(tgroup)

    theads = tgroup.findall("thead")
    tbodys = tgroup.findall("tbody")

    if theads:
        thead = theads[0]
        html_table += "  <thead>\n"
        for row in thead.findall("row"):
            html_table += "    <tr>\n"
            for entry in row.findall("entry"):
                cell_tag = "th"
                cell_attributes = ""

                # Handle `namest` and `nameend` for colspan in headers
                namest = entry.get("namest")
                nameend = entry.get("nameend")
                if namest and nameend:
                    start = get_col_num(namest)
                    end = get_col_num(nameend)
                    colspan = end - start + 1
                    cell_attributes += f' colspan="{colspan}"'

                # Handle `morerows` for rowspan in headers
                morerows = entry.get("morerows")
                if morerows:
                    rowspan = int(morerows) + 1
                    cell_attributes += f' rowspan="{rowspan}"'

                # Get alignment attribute
                # align = entry.get("align")
                # if align:
                #     cell_attributes += f' style="text-align:{align};"'

                # Get content, including handling italic and sub/sup elements
                # content = ''.join(part.text or '' for part in entry.iter())
                content = ''.join((part.text or '') + (part.tail or '') for part in entry.iter())
                # content = get_full_text(entry)
                
                # Add the cell to the header row in HTML format
                html_table += f"      <{cell_tag}{cell_attributes}>{content}</{cell_tag}>\n"
            html_table += "    </tr>\n"
        html_table += "  </thead>\n"

    # Handle table body
    if not tbodys:
        # wtf
        return None, None
    
    tbody = tbodys[0]
    html_table += "  <tbody>\n"
    for row in tbody.findall("row"):
        html_table += "    <tr>\n"
        for entry in row.findall("entry"):
            cell_tag = "td"
            cell_attributes = ""

            # Handle `namest` and `nameend` for colspan in body
            namest = entry.get("namest")
            nameend = entry.get("nameend")
            if namest and nameend:
                start = get_col_num(namest)
                end = get_col_num(nameend)
                colspan = end - start + 1
                cell_attributes += f' colspan="{colspan}"'

            # Handle `morerows` for rowspan in body
            morerows = entry.get("morerows")
            if morerows:
                rowspan = int(morerows) + 1
                cell_attributes += f' rowspan="{rowspan}"'

            # Get alignment attribute
            align = entry.get("align")
            if align:
                cell_attributes += f' style="text-align:{align};"'

            # Get cell content, including handling italic, sub, and sup elements
            # content = ''.join(part.text or '' for part in entry.iter())
            content = ''.join((part.text or '') + (part.tail or '') for part in entry.iter())
            # content = get_full_text(entry)
            
            # Add the cell to the body row in HTML format
            html_table += f"      <{cell_tag}{cell_attributes}>{content}</{cell_tag}>\n"
        html_table += "    </tr>\n"
    html_table += "  </tbody>\n"

    html_table += "</table>"

    return html_table, label_content

# Example usage
if __name__ == "__main__":
    xml_str = """<table>
    <tgroup cols="3">
        <colspec colname="col1" colnum="1"/>
        <colspec colname="col2" colnum="2"/>
        <colspec colname="col3" colnum="3"/>
        <tbody>
        <row>
            <entry namest="col1" nameend="col2">Merged cell 1-2</entry>
            <entry>Cell 3</entry>
        </row>
        <row>
            <entry morerows="1">Row span cell</entry>
            <entry>Cell 2</entry>
            <entry>Cell 3</entry>
        </row>
        <row>
            <entry>Cell 2</entry>
            <entry>Cell 3</entry>
        </row>
        </tbody>
    </tgroup>
    </table>"""

    html_output, label, caption = parse_cals_table(xml_str)

    import pandas as pd
    import io
    df = pd.read_html(io.StringIO(html_output))[0]
    print(html_output)
