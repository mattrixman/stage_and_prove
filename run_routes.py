import sh
import time
import _mysql

import IPython

def wait_on_success(db, request_id, timeout_minutes, callback):
    timeout = time.time() + 60*timeout_minutes
    while time.time() < timeout:
        db.query("""
                 SELECT name, status FROM billing_request WHERE uuid = '{}';
                  """.format(request_id))
        result = db.store_result()
        for b_name, b_status in result.fetch_row():
            name = b_name.decode('UTF-8')
            status = b_status.decode('UTF-8')
            if status == 'SUCCEEDED':
                print("{} {} Completed without errors".format(name, request_id))
                callback()
                return True
            elif status == 'HAS_ERRORS':
                print("{} {} Completed with errors".format(name, request_id))
                return False
        print("Waiting on {}".format(request_id))
        time.sleep(10)
    print("Timed out while waiting for request {} ", request_id)
    return False

def main(args):
    db = _mysql.connect(user="root", host="localhost", db="billing", passwd="test")
