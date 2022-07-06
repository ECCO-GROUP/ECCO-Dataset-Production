import time
import importlib

def run_script(event):
    im_st = time.time()
    script = importlib.import_module('eccov4r4_gen_for_podaac_cloud')
    print(f'DURATION\tIMPORT\t{time.time() - im_st}\tseconds')

    run_st = time.time()
    script.generate_netcdfs(event)
    print(f'DURATION\tRUN\t{time.time() - run_st}\tseconds')

    return


def handler(event, context):
    print('Inside handler')

    all_st = time.time()
    run_script(event)
    print(f'DURATION\tALL\t{time.time() - all_st}\tseconds')

    return