import math


def doi_to_filename(doi: str | int |float |None, file_extension: str = ''): # ".xml"):
    # https://www.crossref.org/documentation/member-setup/constructing-your-dois/suffixes-containing-special-characters/
    if doi is None:
        return None
    if isinstance(doi, (float, int)):
        if math.isnan(doi):
            return None
        return str(int(doi)) + file_extension
    doi = doi.replace(":", r'%3a') # lowercase necessary
    doi = doi.replace('<', '&lt;')
    doi = doi.replace('>', '&gt;')
    return doi.replace('/', '_') + file_extension
    
def filename_to_doi(filename: str):
    # Remove the ".xml" suffix
    # doi = filename[:-4]
    if filename.endswith('.xml'):
        doi = filename[:-4]
    elif filename.endswith('.pdf'):
        doi = filename[:-4]
    else:
        doi = filename
    # Reverse the replacements
    doi = doi.replace('_', '/')
    doi = doi.replace('&gt;', '>')
    doi = doi.replace('&lt;', '<')
    doi = doi.replace('%3A', ':')
    doi = doi.replcae(r'%3a', ':')
    return doi
