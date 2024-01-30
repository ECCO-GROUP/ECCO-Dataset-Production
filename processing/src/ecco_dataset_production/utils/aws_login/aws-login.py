#!/usr/bin/env python3
# Please refer to https://github.jpl.nasa.gov/cloud/Access-Key-Generation

import sys
import os
import boto3
from pytz import timezone
import requests
import getpass
import base64
import logging
import collections
import re
import json
from bs4 import BeautifulSoup
from os.path import expanduser
import lxml.etree as ET
import argparse
import http.cookiejar
import time
import configparser as ConfigParser
from urllib.parse import urlparse
from builtins import input as raw_input
MAX_SLEEP = 5


#uncomment this for lots of debug spew
#boto.set_stream_logging('boto')

def _configure_logging(level):
    logging.basicConfig(format='%(levelname)s  - %(message)s', level=level)

def _get_args_and_settings():
    # Returns a list of script defaults and settings as well as the commandline arguements
    DEFAULTS = {
        "session_timeout" : "14400",
        "version" : "1.4.2.a.2022.06.07",
        "allowed_outputs" : ["json","text","table"],
        "output" : "json",
        "sso_host" : "https://sso3.jpl.nasa.gov/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp=",
        "sslverification" : True,
        "domain" : "JPL",
        "allowed_regions" : ["us-east-1","us-east-2","us-west-1","us-west-2","us-gov-west-1","us-gov-east-1"],
        "auth_method" : {
            "gov" : "saml-gov",
            "pub" : "saml-pub"
        },
        "regions" : {
            "gov" : "us-gov-west-1",
            "pub" : "us-west-2"
        },
        "profiles" : {
            "gov" : "saml-gov",
            "pub" : "saml-pub"
        },
        "provider_id" : {
            "pub" : "urn:amazon:webservices",
            "gov" : "urn:amazon:webservices:govcloud"
        }
    }

    print("")
    print("JPL AWS CLI Login Helper Version " + DEFAULTS['version'])
    print("")

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-pub', '--pub', required=False, help='shortcut to use AWS Pub. (Default profile/region: ' + DEFAULTS['profiles']['pub'] + '/' + DEFAULTS['regions']['pub'] + ')', action='store_true', default=False)
    parser.add_argument('-gov', '--gov', required=False, help='shortcut to use AWS Gov. (Default profile/region: ' + DEFAULTS['profiles']['gov'] + '/' + DEFAULTS['regions']['gov'] + ')', action='store_true', default=False)
    parser.add_argument('-r', '--region', metavar='', type=str, required=False, help='region stored in AWS config and credentials. (Default: ' + DEFAULTS['regions']['pub'] + '|' + DEFAULTS['regions']['gov']+ ')')
    parser.add_argument('-p', '--profile', metavar='', type=str, required=False, help='AWS config profile stored (Default: ' + DEFAULTS['profiles']['pub'] + '|' + DEFAULTS['profiles']['gov'] + ')')
    parser.add_argument('-o', '--output', metavar='', type=str, required=False, help='specify the AWS cli output format [json|text|table] (Default: ' + DEFAULTS['output'] + ')', choices=['json','text','table'])
    parser.add_argument('-t', '--sess_ttl_sec', metavar='', type=int, required=False, help='session duration in seconds, cannot exceed setting in IAM (Default: ' + DEFAULTS['session_timeout'] + ')', default=int(DEFAULTS['session_timeout']),choices=range(14400,0,-3600))
    parser.add_argument('-a', '--arn', metavar='', type=str, required=False, help='specify the ARN of the role and skip role selection screen')
    parser.add_argument('-s', '--sso_url', metavar='', type=str, required=False, help='the JPL SSO URL (Default depends on region: https://sso3.jpl.nasa.gov/...... )', default='')
    parser.add_argument('-U', '--username', metavar='', type=str, required=False, help='JPL username or email used if authentication is required')
    parser.add_argument('-l', '--last_role', required=False, help='automatically use the last role after login. Do not prompt to select a role', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', required=False, help='verbose output', action='store_true', default=False)
    args = parser.parse_args()

    if args.verbose:
        _configure_logging(logging.INFO)
    else:
        _configure_logging(logging.ERROR)

    # Derive AWS Partition from script name; overrides the args.pub or gov
    scriptname = os.path.basename(sys.argv[0])
    if scriptname.lower().find('gov') > 0:
        cloud_type = 'gov'
        args.gov = True
    elif scriptname.lower().find('pub') > 0:
        cloud_type = 'pub'
        args.pub = True
    else:
        cloud_type = None

    if args.pub and args.gov:
        logging.error("Use either -pub or -gov")
        sys.exit(1)
    

    # Even if the AWS partition is derived from the script name, args can override
    if args.gov:
        cloud_type = 'gov'
        if args.region != None and 'gov' not in args.region:
            logging.error('You have specified a region that does not exist in the AWS ' + cloud_type + ' partition.')
            sys.exit(1)
    if args.pub:
        cloud_type = 'pub'
        if args.region != None and 'gov' in args.region:
            logging.error('You have specified a region that does not exist in the AWS ' + cloud_type + ' partition.')
            sys.exit(1)

    if args.region != None and args.region not in DEFAULTS['allowed_regions']:
        logging.error('Invalid AWS region specified. Valid regions are: ' + str(DEFAULTS['allowed_regions']))
        sys.exit(1)
        
    if args.profile == None and (scriptname.lower().find('gov') > 0 or scriptname.lower().find('pub') > 0):
        args.profile = DEFAULTS['profiles'][cloud_type]
    elif args.profile == None and (args.pub or args.gov):
        args.profile = DEFAULTS['profiles'][cloud_type]

    if cloud_type == None and args.region != None:
        if 'gov' in args.region:
            cloud_type = 'gov'
        else:
            cloud_type = 'pub'

    awsdir = expanduser("~") + '/.aws'
    args.awscredsfile = awsdir + '/credentials'
    args.awsconfigfile = awsdir + '/config'
    args.cookiefile = awsdir + '/aws-login_cookies'
    config = _read_config(filename=args.awsconfigfile)
    # if a partition is not specified or derived, we can derive the info from existing profile:
    if (os.environ.get('AWS_DEFAULT_PROFILE') != None or os.environ.get('AWS_PROFILE') != None) and args.profile == None and cloud_type == None:
        args.profile = os.environ.get('AWS_PROFILE') or os.environ.get('AWS_DEFAULT_PROFILE')
        section_name = 'profile '+ args.profile
        if config.has_option(section_name,'region'):
            args.region = config.get(section_name,'region')
            if 'gov' in args.region:
                cloud_type = 'gov'
            else:
                cloud_type = 'pub'
        if config.has_option(section_name,'output') and args.output == None:
            args.output = config.get(section_name,'output')
        
    if args.profile != None and cloud_type == None and config.has_section('profile '+ args.profile):
        section_name = 'profile '+ args.profile
        if config.has_option(section_name,'region'):
            args.region = config.get(section_name,'region')
            if 'gov' in args.region:
                cloud_type = 'gov'
            else:
                cloud_type = 'pub'
        if config.has_option(section_name,'output') and args.output == None:
            args.output = config.get(section_name,'output')


    ## if a partition is not specified or derived, we need to prompt:
    if cloud_type == None:
        print("\nPlease specify an AWS partition:")
        i = 0
        for ea in DEFAULTS['regions'].keys():
            print('    [{}] ---> {}'.format(i, ea))
            i += 1
        try:
            _choice = int(raw_input('Selection: '))
            if 0 <= _choice and _choice <= i:
                cloud_type = list(DEFAULTS['regions'].keys())[_choice]
                logging.debug("Selected partition: " + cloud_type)
            else:
                logging.error ('Invalid AWS Partition selected')
                sys.exit(0)
        except Exception as e:
            logging.error ('Invalid AWS Partition selected!!!!')
            sys.exit(1)


    ## If the script name is aws-login-gov or pub, fall back on using old profile names
    ## while still allowing the user to override the profile, region names as well as
    ## read the existing region name from the config file
    if cloud_type != None:
        if args.profile == None:
            args.profile = DEFAULTS['profiles'][cloud_type]
        if args.region == None:
            if config.has_section('profile '+ args.profile):
                args.region = config.get('profile '+ args.profile,'region')
            else:
                args.region = DEFAULTS['regions'][cloud_type]
        if args.output == None:
            if config.has_section('profile '+ args.profile):
                args.output = config.get('profile '+ args.profile,'output')
            else:
                args.output = DEFAULTS['output']
    else:
        pass
    
    # Try to get the last_arn from the config file:
    section_name = 'profile '+ args.profile
    if config.has_section(section_name):
        if args.arn == None and config.has_option(section_name,'select_role_arn'):
            args.last_arn = config.get(section_name,'select_role_arn')
        else:
            args.last_arn = None
    else:
        args.last_arn = None


    # args varilable is used to the settings as well as command line paramerters
    args.sslverification = DEFAULTS['sslverification']
    args.domain = DEFAULTS['domain']
    args.auth_method = DEFAULTS['auth_method'].get(cloud_type)
    args.sso_url = args.sso_url or  DEFAULTS['sso_host'] + DEFAULTS['provider_id'].get(cloud_type)
    args.version = DEFAULTS['version']

    for arg in vars(args):
        logging.info('{} = {}'.format(arg,getattr(args,arg)))

    return args

def _check_for_error(soup):
    error_list=[]
    found_error = False
    for div in soup.find_all("div", attrs={"class" : "fieldMargin error smallText"}):
        error_list = [text for text in div.stripped_strings]
        
    if len(error_list) > 0:
        logging.error(error_list[0])
        found_error = True

    container = soup.find("div",attrs={'class': 'content','id': 'verification_factor'})
    if container is not None:
        paragraph = container.find("h3")
        if paragraph is not None:
            if paragraph.text == "An Error Occurred":
                found_error = True
                for paragraph in container.find_all("p"):
                    if paragraph.text != "":
                        logging.error(paragraph.text)
    return  found_error

def _write_config(config,filename):
    # Updates a config type file such as the aws config or credentials file
    try:
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(filename, 'w+') as configfile:
            config.write(configfile)
    except Exception as e:
            logging.error('Unable to write the file: ' + filename)
            logging.debug(str(e))
            sys.exit(1)

def _read_config(filename):
    # Reads from a config type file such as the aws config or credentials file
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    try:
        config.read(filename)
    except Exception as e:
        logging.error('Unable to open the file: ' + filename)
        logging.debug(str(e))
    return config

def _get_account_aliases(session, username, password, auth_method, saml_response, signin_url,headers):
    acct_alias_pattern = re.compile("Account: *([^(]+) *\(([0-9]+)\)")
    acct_without_alias_pattern = re.compile("Account: *\(?([0-9]+)\)?")
    alias_response = session.post(
        signin_url,
        verify=True,
        headers=headers,
        auth=None,
        data={
            'UserName': username,
            'Password': password,
            'AuthMethod': auth_method,
            'SAMLResponse': saml_response,
        }
    )

    logging.debug(u'''Request:
        * url: {}
        * headers: {}
    Response:
        * status: {}
        * headers: {}
        * body: {}
    '''.format(signin_url,
               alias_response.request.headers,
               alias_response.status_code,
               alias_response.headers,
               alias_response.text))

    html_response = ET.fromstring(alias_response.text, ET.HTMLParser())

    accounts = {}
    account_element_query = './/div[@class="saml-account-name"]'
    for account_element in html_response.iterfind(account_element_query):
        logging.debug(u'Found SAML account name: {}'.format(account_element.text))
        m = acct_alias_pattern.search(account_element.text)
        if m is not None:
            accounts[m.group(2)] = m.group(1).strip()

        if m is None:
            m = acct_without_alias_pattern.search(account_element.text)
            if m is not None:
                accounts[m.group(1)] = m.group(0).strip()

    return accounts

def _get_assertion(soup):
    assertion = ''
    action = ''
    for inputtag in soup.find_all('input'):
        if(inputtag.get('name') == 'SAMLResponse'):
            logging.debug(inputtag.get('value'))
            assertion = inputtag.get('value')
    
    for inputtag in soup.find_all(re.compile('(FORM|form)')):
        action = inputtag.get('action')

    return assertion, action

def _get_title(html):
    soup = BeautifulSoup(html, 'html.parser')
    title = soup.title
    if title == '' or title is None:
        logging.error("Unable to find the title of the HTML page on ")
    return title.text

def _get_mfaGreeting(text):
    soup = BeautifulSoup(text, 'lxml')
    container = soup.find("div",attrs={'id': 'mfaGreetingDescription'})
    if container is not None:
        text = container.text
    else:
        text = None
    return text

def _dump_debug_to_tmp(text,i):
    # Manual Debug only
    # Turn on logging.DEBUG or lowering this logging.INFO will result in files in tmp/TEMP directory and can contain
    # sensitive info such as password
    if logging.DEBUG >= logging.root.level:
        i = i + 1
        filename =(os.getenv("TEMP") if os.name=="nt" else "/tmp") + os.path.sep + "response-" + str(i) + ".txt"
        with open(filename, 'w', encoding="utf-8") as out:
            out.write(text + '\n')
    return i

def main():
    ##########################################################################
    # Get Args and set variables
    kwargs = _get_args_and_settings()
    logging.info("credential filename path: " + kwargs.awscredsfile)
    jpl_headers = {
        'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate', 'Accept': '*/*', 
        'User-Agent': 'python-requests/2.22.0 (JPL Access-Key-Generation v.' + kwargs.version + ')'}
    role_arn = None
    principal_arn = None
    previous_selected = None
    token = None
    i = 0
    ##########################################################################
    # Get the federated credentials from the user if prompted
    print("Authenticating to AWS and JPL SSO: " + kwargs.sso_url)

    session = requests.Session()
    session.cookies = http.cookiejar.MozillaCookieJar(kwargs.cookiefile)

    if os.path.exists(kwargs.cookiefile):        
        session.cookies.load(ignore_discard=True)
    else:
        try:
            dirname = os.path.dirname(kwargs.cookiefile)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
        except Exception as e:
            logging.error('Unable to create the directory: ' + dirname)
            logging.debug(str(e))
            sys.exit(1)

    # Programmatically get the SAML assertion as it opens the initial IdP url and
    # follows all of the HTTP302 redirects, and gets the resulting login page
    #urllib3.disable_warnings()
    response = session.get(kwargs.sso_url, verify=kwargs.sslverification)
    i = _dump_debug_to_tmp(response.text,i)
    session.cookies.save(ignore_discard=True)
    # Capture the idpauthformsubmiturl, which is the final url after all the 302s
    idpauthformsubmiturl = response.url

    # Parse the response and extract all the necessary values in order to build a
    # dictionary of all of the form values the IdP expects
    soup = BeautifulSoup(response.text, 'html.parser')
    if _check_for_error(soup):
        sys.exit(1)
    needToAuth = True
    assertion, aws_signin_url = _get_assertion(soup)
    if assertion != '':
        logging.info("Using cached authorization info in " + kwargs.cookiefile + " (no need to prompt for credentials)")
        needToAuth = False
    else:
        username = None
        password = os.getenv("AccessKeyGeneration",None)
        payload = {}
        for inputtag in soup.find_all(re.compile('(INPUT|input)')):
            name = inputtag.get('name','')
            value = inputtag.get('value','')
            if name == 'UserName':
                if value == '':
                    if kwargs.username is None and username is None:
                        username = raw_input('Username [' + getpass.getuser() + ']: ')
                        username = username or getpass.getuser()
                    elif username is None and kwargs is not None:
                        username = kwargs.username
                else:
                    needToAuth = False
                    break
                if password is None:
                    password = getpass.getpass('Password: ')    
                    if password == '':
                            logging.error('No password detected. Exiting.')
                            sys.exit(0)
                if '@' in username:
                    payload[name] = username
                else:
                    payload[name] = kwargs.domain + '\\' + username
            elif 'password' in name.lower():
                payload[name] = password
            elif 'Kmsi' in name: 
                payload[name] = 'true'
            else:
                payload[name] = value

        if needToAuth:
            logging.debug('payload1 :')
            logging.debug(payload)

            for inputtag in soup.find_all(re.compile('(FORM|form)')):
                action = inputtag.get('action')
                loginid = inputtag.get('id')
                if (action and loginid == 'loginForm'):
                    parsedurl = urlparse(kwargs.sso_url)
                    idpauthformsubmiturl = parsedurl.scheme + '://' + parsedurl.netloc + action

            response = session.post(idpauthformsubmiturl, data=payload, verify=kwargs.sslverification, headers=jpl_headers)
            i = _dump_debug_to_tmp(response.text,i)
            session.cookies.save(ignore_discard=True)
            del password
            del username
            soup = BeautifulSoup(response.text, 'html.parser')

            #Look for password error and exit
            if _check_for_error(soup):
                sys.exit(1)
        
        #################
        #### Look for RSA Part1:
        ################
        payload = {}
        for inputtag in soup.find_all(re.compile('(INPUT|input)')):
            name = inputtag.get('name','')
            value = inputtag.get('value','')
            # Debug the session data output.
            logging.debug("Seen " + name + " and had " + value)
            if "authmethod" in name.lower():
                payload[name] = "SecurIDv2Authentication"
            elif "context" in name.lower():
                payload["Context"] = value
            elif name == "latitude" or name == "longitude" or name == "geoLocationCollTimestamp" or name == "rsa_risk_fp":
                payload[name] = ""
        
        logging.debug("payload2: ")
        logging.debug(payload)
        # Push pre-auth context and data to page to pull pin entry form.
        response = session.post(idpauthformsubmiturl, data= payload, verify=kwargs.sslverification, headers=jpl_headers)
        i = _dump_debug_to_tmp(response.text,i)
        session.cookies.save(ignore_discard=True)

        #################
        #### Look for RSA Part2:
        ################
        passcode = None
        title = _get_title(html=response.text)
        if title == "" or title is None:
            logging.error("Unable to find the title when decoding " + idpauthformsubmiturl )
            sys.exit(1)
        if title == "RSA SecurID Access Authentication":
            print(_get_mfaGreeting(response.text))
            soup = BeautifulSoup(response.text, 'lxml')
            container = soup.find("div",attrs={'class': 'content','id': 'verification_factor'})
            if container is not None:
                paragraph = container.find("p")
                if paragraph is not None:
                    print(paragraph.text)
                    print("\t",end = '',flush=True)
                else:
                    paragraph = container.find("h3")
                    if paragraph is not None:
                        print(paragraph.text)
                        print("\t",end = '',flush=True)
            approve_msg1 = False
            approve_msg2 = False
            while True:
                title = _get_title(html=response.text)
                if title == "" or title is None:
                    logging.error("Unable to find the title when decoding " + idpauthformsubmiturl )
                    sys.exit(1)
                if title == "Error":
                    print("")
                    logging.error("An error occurred. Contact your administrator for more information.")
                    sys.exit(1)
                if title != "RSA SecurID Access Authentication":
                    break
                soup = BeautifulSoup(response.text, 'html.parser')
                payload = {}
                for inputtag in soup.find_all(re.compile('(INPUT|input)')):
                    name = inputtag.get('name','')
                    value = inputtag.get('value','')
                    # Debug the session data output.
                    logging.debug("Seen " + name + " and had " + value)
                    if "authmethod" in name.lower() and value == "SecurIDv2Authentication":
                        payload[name] = value
                    elif name.lower() == "passcode":
                        passcode = getpass.getpass("RSA PIN + token: ")
                        payload[name] = passcode
                        if len(passcode) < 10:
                            logging.error('RSA PIN + token entered is too short. Exiting.')
                            sys.exit(0)
                    elif name.lower() == "nextcode":
                        passcode = getpass.getpass("You must enter RSA PIN + the Next token: ")
                        payload[name] = passcode
                        if len(passcode) < 10:
                            logging.error('RSA PIN + next token entered is too short. Exiting.')
                            sys.exit(0)
                    elif "context" in name.lower():
                        payload[name] = value
                    elif name == "retryStatus":
                        payload[name] = value
                    elif name == "nextOption":
                        payload[name] = value
                    elif name == "CancelStatus":
                        payload[name] = value
                    elif name == "referenceId":
                        payload[name] = value
                        conf_code = value.split(":",1)
                        if len(conf_code) > 1 and approve_msg1 == True and approve_msg2 == False:
                            print("Confirmation Code for this session is: " + conf_code[1] + " ",end = '',flush=True)
                            approve_msg2 = True
                    elif name == "authMode":
                        authMode = value
                        if authMode == "APPROVE":
                            retry = True
                    elif name == "authStatus":
                        authStatus = value
                        if authMode == "APPROVE" and (authStatus == "VERIFICATION_PENDING:" or authStatus == ""):
                            retry = True
                            if approve_msg1 == False:
                                print("VERIFICATION_PENDING. ",end = '',flush=True)
                                approve_msg1 = True
                        elif "VERIFICATION_FAILED" in authStatus:
                            print("")
                            logging.error("Failed RSA authentication (error code: " + authStatus + ")")
                            sys.exit(1)
                        else:
                            retry = False
                if passcode is None and approve_msg2 == True:
                    print(".",end = '',flush=True)
                    time.sleep(MAX_SLEEP)
                response = session.post(idpauthformsubmiturl, data= payload, verify=kwargs.sslverification, headers=jpl_headers)
                i = _dump_debug_to_tmp(response.text,i)
                session.cookies.save(ignore_discard=True)
        del passcode
        print("")
        #################
        #### No Longer looking for RSA. Do we have SAML
        ################

        soup = BeautifulSoup(response.text, "lxml")
        #Check for Error
        if _check_for_error(soup):
            sys.exit(1)
        # Look for the SAMLResponse attribute of the input tag (determined by
        # analyzing the debug print lines above)
        assertion, aws_signin_url = _get_assertion(soup)

    if (assertion == ''):
        #TODO: Insert valid error checking/handling
        logging.error('Failed authentication (error code: InvalidSAML)')
        sys.exit(1)

    # Manual Debug only
    logging.debug(base64.b64decode(assertion))

    # Parse the returned assertion and extract the authorized roles
    awsroles = []
    root = ET.fromstring(base64.b64decode(assertion))
    for saml2attribute in root.iter('{urn:oasis:names:tc:SAML:2.0:assertion}Attribute'):
        if (saml2attribute.get('Name') == 'https://aws.amazon.com/SAML/Attributes/Role'):
            for saml2attributevalue in saml2attribute.iter('{urn:oasis:names:tc:SAML:2.0:assertion}AttributeValue'):
                awsroles.append(saml2attributevalue.text)

    for awsrole in awsroles:
        chunks = awsrole.split(',')
        if'saml-provider' in chunks[0]:
            newawsrole = chunks[1] + ',' + chunks[0]
            logging.debug(newawsrole)
            index = awsroles.index(awsrole)
            awsroles.insert(index, newawsrole)
            awsroles.remove(awsrole)


    print('')

    # If an ARN is specified, validate the ARN and find the principal
    if kwargs.arn != None :
        for awsrole in awsroles:
            if awsrole.split(',')[0] == kwargs.arn:
                role_arn = kwargs.arn
                principal_arn = awsrole.split(',')[1]
                break
        if role_arn == None or principal_arn == None:
            logging.error('Invalid role ARN provided.')
            sys.exit(1)

    # If I have more than one role, ask the user which one they want,
    # otherwise just proceed
    elif len(awsroles) > 1 and kwargs.arn == None:
        logging.info("Getting a list of account aliases and creating an aggregated list of roles and accounts")
        account_aliases = _get_account_aliases(session=session,username='username',password='password',auth_method=kwargs.auth_method,saml_response=assertion,signin_url=aws_signin_url, headers=jpl_headers)
        aggregated_accounts = {}
        for awsrole in awsroles:
            role_arn = awsrole.split(',')[0]
            principal_arn = awsrole.split(',')[1]
            role_name = role_arn.split(':role/')[1]
            account_no = role_arn.split(':')[4]

            if account_no not in account_aliases:
                account_aliases[account_no] = account_no

            if account_aliases[account_no] not in aggregated_accounts:
                aggregated_accounts[account_aliases[account_no]] = {}
            aggregated_accounts[account_aliases[account_no]][role_arn] = {'name': role_name, 'principal_arn': principal_arn}

        logging.debug(json.dumps(aggregated_accounts, indent=4,  sort_keys=True))
        aggregated_accounts = collections.OrderedDict(sorted(aggregated_accounts.items(), key=lambda t: t[0]))
        i = 0
        awsroles = []
        if not kwargs.last_role:
            print('Please choose the role you would like to assume:')
        for account_name in aggregated_accounts.keys():
            roles = aggregated_accounts[account_name]
            if not kwargs.last_role:
                print('{}:'.format(account_name))
            for role_arn in roles.keys():
                if kwargs.last_arn != None and role_arn == kwargs.last_arn:
                    previous_selected=i
                    logging.info('Found the previously used role in the list')
                role_entry = roles[role_arn]
                if not kwargs.last_role:
                    print('    [{:2d}] {}-> {}'.format(i, role_entry['name'].ljust(30, '-'), role_arn))
                i += 1
                awsroles.append(role_arn + ',' + role_entry['principal_arn'])
        if not kwargs.last_role:
            print('')
            print('    [ q] Quit\n')
            sys.stdout.write('Selection') # python2/3 compat print w/o newline
            sys.stdout.flush()
            if previous_selected != None:
                selectedroleindex = raw_input(' [' + str(previous_selected) + ']: ')
                if selectedroleindex.lower() == 'q':
                    sys.exit(0)
                selectedroleindex = selectedroleindex or previous_selected
            else:
                selectedroleindex = raw_input(': ')
                if selectedroleindex.lower() == 'q':
                    sys.exit(0)
            
        if kwargs.last_role:    
            selectedroleindex = previous_selected

        if selectedroleindex == '' or selectedroleindex == None:
            logging.error('You have not selected a valid role, please try again')
            sys.exit(0)
        elif int(selectedroleindex) > (len(awsroles) - 1):
            logging.error('You selected an invalid role index, please try again')
            sys.exit(0)

        role_arn = awsroles[int(selectedroleindex)].split(',')[0]
        principal_arn = awsroles[int(selectedroleindex)].split(',')[1]
    else:
        role_arn = awsroles[0].split(',')[0]
        principal_arn = awsroles[0].split(',')[1]

    logging.info(role_arn)
    logging.info(principal_arn)

    # Need to make sure that there is at least a section for the profile in
    # AWS creds file. Otherwise, you will get a boto3 exception. This also
    # handles the case where there is an env variable for AWS_DEFAULT_PROFILE 
    config = _read_config(filename=kwargs.awscredsfile)
    section_name = kwargs.profile
    if not config.has_section(section_name):
        config.add_section(section_name)
        _write_config(config=config, filename=kwargs.awscredsfile)

    # Use the assertion to get an AWS STS token using Assume Role with SAML
    session = boto3.Session(region_name=kwargs.region,profile_name=kwargs.profile)
    stsclient = session.client('sts', region_name=kwargs.region)
  
    while kwargs.sess_ttl_sec >= 3600:
        try:
            response = stsclient.assume_role_with_saml(RoleArn=role_arn,PrincipalArn=principal_arn,SAMLAssertion=assertion,DurationSeconds=kwargs.sess_ttl_sec)
            token = response['Credentials']
            kwargs.sess_ttl_sec = 0
        except Exception as e:
            logging.debug("trying a lower session timeout")
            kwargs.sess_ttl_sec = kwargs.sess_ttl_sec - 3600
    if token == None:
        logging.error("Unable to get a STS token. Please double check the region, role-arn, and timeout")
        sys.exit(1)
    # Write and update the AWS credential file with the STS token
    exp_date = token['Expiration'].astimezone(timezone('US/Pacific'))

    config.set(section_name, 'output', kwargs.output)
    config.set(section_name, 'region', kwargs.region)
    config.set(section_name, 'aws_access_key_id', token['AccessKeyId'])
    config.set(section_name, 'aws_secret_access_key', token['SecretAccessKey'])
    config.set(section_name, 'aws_session_token', token['SessionToken'])
    config.set(section_name, '#Expiration Date', exp_date)
    _write_config(config=config, filename=kwargs.awscredsfile)

    # Write and update the AWS config file
    config = _read_config(filename=kwargs.awsconfigfile)
    section_name = 'profile '+ kwargs.profile
    if not config.has_section(section_name):
        config.add_section(section_name)
    config.set(section_name, 'output', kwargs.output)
    config.set(section_name, 'region', kwargs.region)
    config.set(section_name,'select_role_arn', role_arn)

    _write_config(config=config, filename=kwargs.awsconfigfile)

    # Give the user some basic info as to what has just happened
    print('\n\n----------------------------------------------------------------')
    print('Credential file {0} has been successfully updated. To use you must specify the profile \'{1}\'.'.format(kwargs.awsconfigfile,kwargs.profile))
    print('\nFor example:')
    print('$ aws --profile {0} sts get-caller-identity'.format(kwargs.profile))
    print('----------------------------------------------------------------\n\n')

    if kwargs.verbose:
        # Use the AWS STS token to list all of the S3 buckets
        s3client = session.client('s3',
                        aws_access_key_id=token['AccessKeyId'],
                        aws_secret_access_key=token['SecretAccessKey'],
                        aws_session_token = token['SessionToken'],
                        region_name = kwargs.region)

        print('Simple API example listing all S3 buckets:')
        buckets = s3client.list_buckets()['Buckets']
        for bucket in buckets:
            print(bucket['Name'])   


if __name__ == "__main__":
    main()