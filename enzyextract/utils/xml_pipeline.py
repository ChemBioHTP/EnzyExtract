import bs4


junk = [f'xmlns="http://www.elsevier.com/xml/common/dtd"']
def xml_get_soup(raw_text) -> bs4.BeautifulSoup | None:
    """
    Accepts raw text from an xml file.
    Returns soup, raw_text, tables
    :param raw_text:
    :return:
    soup: a bs4.BeautifulSoup object
    raw_text: a string with all the text
    tables: a list of strings of LLM-readable tables
    """

    if not '<body' in raw_text and not '<xocs:rawtext' in raw_text:
        return None
        # raise ValueError("No <body> or <xocs:rawtext> found in xml ")
    # remove junk
    for j in junk:
        raw_text = raw_text.replace(j, '')

    # idk why, but html.parser kept hanging 
    soup = bs4.BeautifulSoup(raw_text, "lxml") # avoid putting in head/body tags
    return soup

elename = {}
def xml_raw_text_processing(soup: bs4.BeautifulSoup) -> str:
    """
    Obtain plaintext
    :param soup:
    :return:
    """
    # look for either <xocs:rawtext or <body
    body = soup.find('body')
    analyte = body
    if body is None:
        analyte = soup.find('xocs:rawtext')
    if analyte is None:
        return ''
    
    # Remove <ce:bibliography> elements
    for bibliography in analyte.find_all('ce:bibliography'):
        bibliography.decompose()
    
    # Capitalize all content inside <ce:small-caps>
    for small_caps in analyte.find_all('ce:small-caps'):
        if small_caps.string:  # If it's a single text node
            small_caps.string = small_caps.string.upper()
        # raise ValueError("No <body> or <xocs:rawtext> found in xml.")
    # https://stackoverflow.com/questions/17530471/get-all-text-from-an-xml-document

    # print all strings

    # instead of conglomerating all strings, try a more fine-grained approach
    ws_map = {"ce:para": "\n\n", "ce:section": "\n\n", "ce:simplesect": "\n\n",
              "ce:italic": "", "ce:bold": "", "ce:sup": "", "ce:sub": "", "ce:sc": "", "ce:underline": "",
              "ce:inf": "", "ce:cross-ref": "", "ce:small-caps": ""}
    # all else: \n
    # print all strings

    builder = ''

    for ele in analyte.descendants:
        # exclude ce:bibliography
        if ele.name and 'ce:' in ele.name:
            elename[ele.name] = elename.get(ele.name, 0) + 1
        if isinstance(ele, bs4.element.NavigableString):
            builder += ele.replace('\n',' ')
        elif isinstance(ele, bs4.element.Tag):
            # Capitalize content inside <ce:small-caps>
            if ele.name in ws_map:
                builder += ws_map[ele.name]
            elif ele.name and not ele.name.startswith('ce:'):
                # check the last token in builder
                if builder and builder[-1] not in [' ', '\n']:
                    builder += " "
    return builder

class TraversalHandler:
    builder = ""
    def enter(self, node):
        pass
    def exit(self, node):
        pass

class TableTraversalHandler(TraversalHandler):
    builder = ""

    def enter(self, node):
        if node.name == 'ce:table':
            self.builder += "<table>"
        elif node.name == 'row':
            self.builder += "\n<row> "
        elif node.name == 'entry':
            self.builder += ""

    def exit(self, node):
        if node.name == 'ce:table':
            self.builder += "</table>"
        elif node.name == 'row':
            self.builder += "</row>"
        elif node.name == 'entry':
            self.builder += ", "

        if isinstance(node, bs4.element.NavigableString):
            self.builder += node.replace('\n', '')

def dfs(node: bs4.PageElement, handler: TraversalHandler):
    # further resources: non-recursive post-order dfs on Wikipedia
    # https://stackoverflow.com/questions/1294701/post-order-traversal-of-binary-tree-without-recursion
    # https://en.wikipedia.org/wiki/Tree_traversal#Post-order_implementation
    # https://www.geeksforgeeks.org/iterative-postorder-traversal-of-n-ary-tree/
    # another option is to use flags (ie. an object to push to the stack to indicate that you should do postprocess)

    handler.enter(node)
    if isinstance(node, bs4.element.Tag):
        for child in node.children:
            dfs(child, handler)
    handler.exit(node)
    return handler.builder

def xml_table_processing(soup: bs4.BeautifulSoup) -> list[str]:
    """
    Processes xml tables
    :param soup: input document
    :return: a list of strings of LLM-readable tables
    """
    tables = soup.find_all('ce:table')
    ret = []
    for table in tables:
        handler = TableTraversalHandler()
        dfs(table, handler)
        ret.append(handler.builder)
    return ret


def xml_abstract_processing(soup: bs4.BeautifulSoup):
    """
    Processes xml abstract
    :param soup: input document
    :return: a string of the abstract
    """
    abstract = soup.find('ce:abstract')
    if abstract is None:
        return ''
    return abstract.get_text(strip=True, separator=' ')
