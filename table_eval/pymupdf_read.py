import pymupdf

doc = pymupdf.open("D:/brenda/wiley/10092863.pdf")

print(doc[1].get_text('text')) # on page 2

