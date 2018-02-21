#  Run the specified route, wait for it to complete, then prove it

import sys
import os
import time
import re
import argparse
import datetime
import string
import subprocess
import _mysql
from collections import namedtuple

# is this a valid date?
def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)

# is this a valid file?
def valid_file(s):
    if not os.path.exists(s):
        msg = "File does not exist: " + s
        raise argparse.ArgumentTypeError(msg)
    else:
        return s

# is this a valid properties file?
Props = namedtuple('Properties', 'host db user password filename')
def valid_props(s):
    valid_file(s)

    billingDbHosMaskt =     re.compile('billingDbHost=(.*)')
    billingDbNameMask =     re.compile('billingDbName=(.*)')
    billingDbUserMask =     re.compile('billingDbUser=(.*)')
    billingDbPasswordMask = re.compile('billingDbPassword=(.*)')
    with open(s) as prop_file:
        props = prop_file.read()

    hostMatch = re.search(billingDbHosMaskt, props)
    nameMatch = re.search(billingDbNameMask, props)
    userMatch = re.search(billingDbUserMask, props)
    passMatch = re.search(billingDbPasswordMask, props)

    if hostMatch and nameMatch and userMatch and passMatch:
        return Props(hostMatch.group(1),
                     nameMatch.group(1),
                     userMatch.group(1),
                     passMatch.group(1),
                     s)
    else:
        msg = "Invalid props file: " + s
        raise argparse.ArgumentTypeError(msg)

provable_routes = {
        "Adjustment", 
        "Advance",
        "Daily", 
        "Metered",
        "Monthly",
        "Subscription",
        "Updown"
        }

# is this a valid route name?
def valid_route(s):
    routeName = string.capwords(s.lower())
    if routeName in provable_routes:
        return routeName
    else:
        msg = "Unknown route: " + s
        raise argparse.ArgumentTypeError(msg)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("route", type=valid_route, 
            help="The route to run synchronously")

    parser.add_argument('-d', '--dueDate', 
            type=valid_date,
            default=datetime.date.today(),
            help="The due date for the route")

    parser.add_argument('-b', '--bsUtils', type=valid_file, 
            default='/opt/clover/billing/test-tools/bin/bsUtils.sh',
            help="Source this bsUtils before running routes")

    parser.add_argument('-p', '--profile', type=valid_props, 
            default='/opt/clover/configs/billing.properties',
            help="Align with this properties file")

    parser.add_argument('-t', '--timeout', type=int, 
            default=60,
            help="Timeout after this many minutes")

    return parser.parse_args()

# poll the database, block until the route is successful
# print status accordingly
def wait_on_success(db, request_id, timeout_minutes):
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
                return True
            elif status == 'HAS_ERRORS':
                print("{} {} Completed with errors".format(name, request_id))
                return False
        print("Waiting on {}".format(request_id))
        time.sleep(10)
    print("Timed out while waiting for request {} ".format(request_id))
    return False

# run the specified command, get the route id, wait for it to complete
def run_wait(cmd, db, args):
    print("Starting: `{}`".format(cmd))
    request_id = subprocess.check_output(['bash', '-c', cmd]).decode('ASCII')

    mask = re.compile(r'[A-Z0-9]{10,15}')
    if re.match(mask, request_id):
        print("Route {} is running".format(request_id))
        if wait_on_success(db, request_id, args.timeout):
            return request_id
        else:
            # timeout
            sys.exit(1)
    else:
        # invalid output
        print("Unexpected response: {}".format(request_id))
        sys.exit(2)



def main(args):

    # we'll need a db connection
    db = _mysql.connect(user=args.profile.user, 
                        host=args.profile.host, 
                        db=args.profile.db, 
                        passwd=args.profile.password)

    # run the route
    stg_cmd = '. {} {} ; bsrun{} -d {}'.format(args.bsUtils, args.profile.filename, args.route, args.dueDate)
    stg_request = run_wait(stg_cmd, db, args)

    # prove the route
    prv_cmd = '. {} {} ; bsprove{} -r {} -d {}'.format(
            args.bsUtils, args.profile.filename, args.route, stg_request, args.dueDate)
    prove_request = run_wait(prv_cmd, db, args)


if __name__ == '__main__':
    args = parse_args()
    main(args)
