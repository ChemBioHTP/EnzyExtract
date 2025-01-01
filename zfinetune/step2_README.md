# Step 2

Step 2 involves a mix of tasks that are needed to produce materials for finetuning.

1. Run ReOCR and put the final parquet file at the right place.
2. Run table collection, and collect .info files and just make sure that you have markdown files of the tables.
3. Run dry_run_tableboth.py and configure it to write a mock batch file in zfinetune/inputs. Prompt doesn't matter, only that the docs are preprocessed accordingly.
4. Of course, make sure that your annotated md file is ready.
