import importlib

def run_script(event):
    script = importlib.import_module('eccov4r4_gen_for_podaac_cloud')

    script.generate_netcdfs(event)

    return


def handler(event, context):
    print('Inside handler')

    run_script(event)
    
    return