from __future__ import print_function
from scrapyd_client.utils import get_request, post_request
from scrapyd_client.utils import indent
from scrapyd_client import lib
import fnmatch
import sys
from argparse import ArgumentParser
from traceback import print_exc
from requests.exceptions import ConnectionError
from scrapyd_client import commands, schedule_estate
from scrapyd_client.utils import ErrorResponse, MalformedRespone, get_config


DEFAULT_TARGET_URL = 'http://localhost:6800'
ISSUE_TRACKER_URL = 'https://github.com/scrapy/scrapyd-client/issues'


def get_projects(url, pattern='*'):
    """ Returns the project names deployed in a Scrapyd instance.
        :param url: The base URL of the Scrapd instance.
        :type url: str
        :param pattern: A globbing pattern that is used to filter the results,
                        defaults to '*'.
        :type pattern: str
        :rtype: A list of strings.
    """
    response = get_request(url + '/listprojects.json')
    return fnmatch.filter(response['projects'], pattern)

def get_spiders(url, project, pattern='*'):
    """ Returns the list of spiders implemented in a project.
        :param url: The base URL of the Scrapd instance.
        :type url: str
        :param project: The name of the project.
        :type project: str
        :param pattern: A globbing pattern that is used to filter the results,
                        defaults to '*'.
        :type pattern: str
        :rtype: A list of strings.
    """
    response = get_request(url + '/listspiders.json',
                           params={'project': project})

    match = fnmatch.filter(response['spiders'], pattern)
 
    return list(set(response['spiders']) - set(match))

def schedule_all(url, project, spider, args={}):
    """ Schedules a spider to be executed.
        :param url: The base URL of the Scrapd instance.
        :type url: str
        :param project: The name of the project.
        :type project: str
        :param spider: The name of the spider.
        :type project: str
        :param args: Extra arguments to the spider.
        :type pattern: mapping
        :returns: The job id.
        :rtype: str
    """
    data = args.copy()
    data.update({'project': project, 'spider': spider})
    response = post_request(url + '/schedule.json', data=data)
    return response['jobid']
    
def schedule(args):
    """ Schedules the specified spider(s). """
    job_args = dict((x[0], x[1]) for x in (y.split('=', 1) for y in args.arg))
    _projects = get_projects(args.target, args.project)
    for project in _projects:
        _spiders = get_spiders(args.target, project, args.spider)
        for spider in _spiders:
            job_id = schedule_all(args.target, project, spider, job_args)
            print('{} / {} => {}'.format(project, spider, job_id))


def parse_cli_args(args):
    target_default = get_config('deploy', 'url', fallback=DEFAULT_TARGET_URL).rstrip('/')
    project_default = get_config('deploy', 'project', fallback=None)
    project_kwargs = {
        'metavar': 'PROJECT', 'required': True,
        'help': 'Specifies the project, can be a globbing pattern.'
    }
    if project_default:
        project_kwargs['default'] = project_default

    description = 'A command line interface for Scrapyd.'
    mainparser = ArgumentParser(description=description)
    subparsers = mainparser.add_subparsers()
    mainparser.add_argument('-t', '--target', default=target_default,
                            help="Specifies the Scrapyd's API base URL.")

    parser = subparsers.add_parser('schedule', description=schedule.__doc__)
    parser.set_defaults(action=schedule)
    parser.add_argument('-p', '--project', **project_kwargs)
    parser.add_argument('spider', metavar='SPIDER',
                        help='Specifies the spider, can be a globbing pattern.')
    parser.add_argument('--arg', action='append', default=[],
                        help='Additional argument (key=value), can be specified multiple times.')

    # TODO remove next two lines when 'deploy' is moved to this module
    parsed_args, _ = mainparser.parse_known_args(args)
    if getattr(parsed_args, 'action', None) is not commands.deploy:
        parsed_args = mainparser.parse_args(args)

    if not hasattr(parsed_args, 'action'):
        mainparser.print_help()
        raise SystemExit(0)

    return parsed_args


def main():
    try:
        args = parse_cli_args(sys.argv[1:])
        args.action(args)
    except KeyboardInterrupt:
        print('Aborted due to keyboard interrupt.')
        exit_code = 0
    except SystemExit as e:
        exit_code = e.code
    except ConnectionError as e:
        print('Failed to connect to target ({}):'.format(args.target))
        print(e)
        exit_code = 1
    except ErrorResponse as e:
        print('Scrapyd responded with an error:')
        print(e)
        exit_code = 1
    except MalformedRespone as e:
        text = str(e)
        if len(text) > 120:
            text = text[:50] + ' [...] ' + text[-50:]
        print('Received a malformed response:')
        print(text)
        exit_code = 1
    except Exception:
        print('Caught unhandled exception, please report at {}'.format(ISSUE_TRACKER_URL))
        print_exc()
        exit_code = 3
    else:
        exit_code = 0
    finally:
        raise SystemExit(exit_code)

__all__ = [get_projects.__name__,
           get_spiders.__name__,
           schedule.__name__]