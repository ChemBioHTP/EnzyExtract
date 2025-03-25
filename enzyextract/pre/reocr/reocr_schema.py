import polars as pl

reocr_df_schema = ['pdfname', 'pageno', 'ctr', 'orig_char', 'orig_after', 'real_char', 'confidence', 'angle', 'letter_x0', 'letter_y0', 'letter_x1', 'letter_y1', 'x0', 'y0', 'x1', 'y1']

reocr_df_schema_overrides = {
    'pdfname': pl.Utf8, 
    'pageno': pl.Int64, 
    'ctr': pl.Int64, 
    'orig_char': pl.Utf8, 
    'orig_after': pl.Utf8, 
    'real_char': pl.Utf8, 
    'confidence': pl.Float64, 
    'angle': pl.Float64, 
    'letter_x0': pl.Float64, 
    'letter_y0': pl.Float64, 
    'letter_x1': pl.Float64, 
    'letter_y1': pl.Float64,
    'x0': pl.Float64, 'y0': pl.Float64, 'x1': pl.Float64, 'y1': pl.Float64
}