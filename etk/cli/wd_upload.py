import requests
from requests.auth import HTTPBasicAuth


def add_arguments(parser):
    parser.add_argument('-e', '--endpoint', action='store', type=str, dest='endpoint')
    parser.add_argument('-f', '--input', action='store', type=str, dest='file')
    parser.add_argument('--user', action='store', default=None, type=str, dest='user')
    parser.add_argument('--passwd', action='store', default=None, type=str, dest='passwd')


def run(args):
    print('Start uploading file {} to endpoint {}'.format(args.file, args.endpoint))
    headers = {
        'Content-Type': 'application/x-turtle',
    }
    response = requests.post(args.endpoint, data=open(args.file).read(), headers=headers,
                             auth=HTTPBasicAuth(args.user, args.passwd))

    print('Upload finished with status code: {}!'.format(response.status_code))
    if response.status_code // 100 != 2:
        print(response.content)

