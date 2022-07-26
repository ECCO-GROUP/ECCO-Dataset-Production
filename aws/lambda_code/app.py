import time
import logging
import importlib

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def run_script(event):
    processing_file = event['product_generation_config']['processing_code_filename']
    print(f'Importing file: {processing_file}')
    im_st = time.time()
    script = importlib.import_module(processing_file)
    logger.info(f'DURATION\tIMPORT\t{time.time() - im_st}\tseconds')

    run_st = time.time()
    script.generate_netcdfs(event)
    logger.info(f'DURATION\tRUN\t{time.time() - run_st}\tseconds')

    return


def handler(event, context):
    logger.info('START')
    print('Inside handler')

    all_st = time.time()
    run_script(event)
    logger.info(f'DURATION\tALL\t{time.time() - all_st}\tseconds')

    return