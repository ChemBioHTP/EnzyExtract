import polars as pl
import os
import json
from tqdm import tqdm
from PIL import Image

from enzyextract.submit.openai_schema import to_openai_batch_request_with_schema
from enzyextract.submit.batch_utils import write_to_jsonl, chunked_write_to_jsonl
from enzyextract.submit.openai_management import submit_batch_file

manifest = pl.read_parquet('data/manifest.parquet')
manifest = manifest.filter(
    pl.col('apogee_kcat')
    & pl.col('readable')
    & ~pl.col('bad_ocr')
    & (pl.col('toplevel') == 'brenda')
).with_columns(
    pl.col('canonical').str.replace(r'\.pdf$', '').alias('pmid')
)


yamls = pl.read_parquet('data/gpt/apogee_gpt.parquet')
yamls = yamls.filter(
    pl.col('pmid').is_in(manifest.select('pmid'))
    & (pl.col('toplevel') == 'brenda')
)
yamls = yamls.with_columns(
    pl.col('content').str.splitn(r'```', 3).alias('code_blocks')
).with_columns(
    pl.col('code_blocks').struct.field('field_1').str.replace("^yaml\s*", "").alias('yaml'),
    pl.col('code_blocks').struct.field('field_2').str.contains('```').alias('has_more')
)

yamls = yamls.filter(
    ~pl.col('has_more')
    & pl.col('yaml').is_not_null()
) # .select('pmid', 'yaml')
# print(yamls)

info = pl.read_parquet('zpreprocessing/data/table_manifest_info.parquet')
info = info.filter(
    pl.col('pmid').is_in(yamls.select('pmid'))
    & ~pl.col('fileroot').str.contains('false_positive')
)
info = info.with_columns(
    pl.col('filename').str.replace(r'\.info$', '').alias('no_ext'),
    pl.col('filename').map_elements(lambda x: x.rsplit('_', 1)[0], return_dtype=pl.Utf8).alias('pmid')
)

# load known images
known_images = []
known_images_root = 'C:/conjunct/tmp/eval/cherry_prod/table_images'
for filename in os.listdir(known_images_root):
    if filename.endswith('.png'):
        known_images.append((known_images_root, filename, filename[:-4]))
images_df = pl.DataFrame(known_images, orient='row', schema=['fileroot', 'filename', 'no_ext'])
images_df = images_df.with_columns([
    pl.col('no_ext').map_elements(lambda x: x.rsplit('_', 1)[0], return_dtype=pl.Utf8).alias('pmid')
])

need_images = info.filter(
    ~pl.col('no_ext').is_in(images_df.select('no_ext'))
).rename({'filename': 'info_filename', 'fileroot': 'info_fileroot'})

# obtain pdf locations
# if need_images.height:
#     need_images = need_images.join(manifest.select(['pmid', 'fileroot']).rename({'fileroot': 'pdfroot'}), how='left', on='pmid')
#     from gmft.auto import RotatedCroppedTable
#     from gmft_pymupdf import PyMuPDFDocument

#     for inforoot, infoname, pdfroot, pmid in tqdm(need_images.select(['info_fileroot', 'info_filename', 'pdfroot', 'pmid']).iter_rows()):
#         doc = PyMuPDFDocument(f"{pdfroot}/{pmid}.pdf")
#         with open(f"{inforoot}/{infoname}", 'r') as f:
#             info = json.load(f)
#         page = doc[info['page_no']]
#         rct = RotatedCroppedTable.from_dict(info, page)
#         img = rct.image(dpi=144)
#         img.save(f"{known_images_root}/{infoname[:-5]}.png")

from enzyextract.prompts import for_vision

yamls_dup = yamls.filter(yamls.select('pmid').is_duplicated()) # only 2

yamls = yamls.unique('pmid', maintain_order=True)


imgs_together = images_df.group_by('pmid').agg([
    (pl.col('fileroot') + '/' + pl.col('filename')).alias('imglocs')
])

joint = yamls.join(imgs_together, on='pmid', how='inner')

batch = []
ctr = 0
namespace = "tablevision-prod1"
for i, (pmid, yaml, imglocs) in enumerate(tqdm(joint.select(['pmid', 'yaml', 'imglocs']).iter_rows(), total=joint.height)):
    imgs = []

    if '\ncontext:' in yaml:
        yaml = yaml.split('\ncontext:')[0]

    if not imglocs:
        continue
    for imgloc in imglocs:
        # load from PIL
        img = Image.open(imgloc)
        imgs.append(img)
    
    # docs = [yaml] + imgs
    docs = imgs
    # req = to_openai_batch_request_with_schema(f"{namespace}_{i}_{pmid}", for_vision.for_vision, docs, 'gpt-4o',
    #     detail='high', schema=for_vision.VisionCorrectionSchema)
    req = to_openai_batch_request_with_schema(f"{namespace}_{i}_{pmid}", for_vision.cls_vision, docs, 'gpt-4o',
        detail='auto', schema=for_vision.VisionClsSchema)
    batch.append(req)
    # debug
    # if ctr > 10:
    #     break

# submit to openAI

dest_filepath = f'batches/revision/{namespace}.jsonl' # + namespace + '.jsonl'

chunk_dests = chunked_write_to_jsonl(batch, dest_filepath, 1000)
for chunk_dest in chunked_write_to_jsonl:
    try:
        batchname = submit_batch_file(chunk_dest, pending_file='batches/pending.jsonl') # will ask for confirmation
    except Exception as e:
        print("Error submitting batch", chunk_dest, e)
# write_to_jsonl(batch, will_write_to)
# batchname = submit_batch_file(will_write_to, pending_file='batches/pending.jsonl')