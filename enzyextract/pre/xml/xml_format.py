
from enzyextract.pre.xml.xml_pipeline import xml_abstract_processing, xml_get_soup, xml_raw_text_processing, xml_table_processing


def process_xml(filepath, original_tables=True):
    """
    original_tables: if True, will give tables as original xml.
    If False, will give tables as an abridged version
    If None: will skip tables
    """
    with open(filepath, "r", encoding='utf-8') as f:
        soup = xml_get_soup(f.read())

    # see EnzyXMLDocumentLoader.lazy_load
    # TODO search for xocs:ucs-locator
    if not soup:
        # print("Could not parse", filepath)
        return None

    # assume that the abstract fits into one chunk ;-;
    abstract = xml_abstract_processing(soup)
    
    # extract raw texts
    raw_txt = xml_raw_text_processing(soup)
    
    docs = []
    if original_tables:
        tables = soup.find_all('ce:table')
        # give each table directly, as raw xml
        for table in tables:
            raw_table = str(table)
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/dtd"', '')
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/cals/dtd"', '')
            if raw_table.strip():
                docs.append(raw_table)
# raw_table = f"""
# Tables: ```xml
# {raw_table}
# ```
# """
    elif original_tables is False:
        # use old table extraction method
        for raw_table in xml_table_processing(soup):
            docs.append(raw_table)
    elif original_tables is None:
        pass
    
    docs.append(f"Abstract: {abstract}")
    docs.append(f"Text: {raw_txt}")
    
    # if not alt_re.search(combined):
    #     if not any(kcat_re.search(combined) for kcat_re in kcat_re_s):
    #         continue
    # convert docs to langchain docs
    return docs


def get_pure_tables(filepath, original_tables=True):
    """
    only extract the xml tables from the filepath
    """
    from lxml import etree


    with open(filepath, "r", encoding='utf-8') as f:
        # Parse the XML directly with lxml
        tree = etree.parse(f)

    tables = []
    # dfs = []
    if original_tables:
        # Use XPath to find all `ce:table` elements
        namespaces = {
            'ce': 'http://www.elsevier.com/xml/common/dtd',  # Update with the actual namespace if different
        }
        table_elements = tree.xpath('//ce:table', namespaces=namespaces)
        
        for table in table_elements:
            # Convert each table element back to a string without unnecessary namespaces
            raw_table = etree.tostring(table, encoding='unicode')
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/dtd"', '')
            raw_table = raw_table.replace(' xmlns="http://www.elsevier.com/xml/common/cals/dtd"', '')
            
            if raw_table.strip():  # Avoid empty entries
                tables.append(raw_table)
            
            # see if pandas can parse it
            # try:
            #     df = pd.read_xml(io.StringIO(raw_table))
            #     if df is not None:
            #         dfs.append(df)
            # except Exception as e:
            #     print("Error parsing table")
            #     print(e)

    return tables