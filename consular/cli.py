import click
import sys

from urllib import urlencode


@click.command()
@click.option('--scheme', default='http',
              help='The scheme to use. (HTTP/HTTPS)')
@click.option('--host', default='localhost',
              help='The host to bind to.')
@click.option('--port', default='7000', type=int,
              help='The port to listen to.')
@click.option('--consul', default='http://localhost:8500',
              help='The Consul HTTP API')
@click.option('--marathon', default='http://localhost:8080',
              help='The Marathon HTTP API')
@click.option('--registration-id',
              help=('Auto register for Marathon event callbacks with the '
                    'registration-id. Must be unique for each consular '
                    'process.'), type=str)
@click.option('--sync-interval',
              help=('Automatically sync the apps in Marathon with what\'s '
                    'in Consul every _n_ seconds. Defaults to 0 (disabled).'),
              type=int)
@click.option('--purge/--no-purge',
              help=('Automatically purge dead services from Consul if they '
                    'are not known in Marathon '
                    '(needs sync-interval enabled).'),
              default=False)
@click.option('--logfile',
              help='Where to log output to',
              type=click.File('a'),
              default=sys.stdout)
@click.option('--debug/--no-debug',
              help='Log debug output or not',
              default=False)
def main(scheme, host, port,
         consul, marathon, registration_id,
         sync_interval, purge, logfile, debug):  # pragma: no cover
    from consular.main import Consular
    from twisted.internet.task import LoopingCall

    consular = Consular(consul, marathon)
    consular.debug = debug
    if registration_id:
        events_url = "%s://%s:%s/events?%s" % (
            scheme, host, port,
            urlencode({
                'registration': registration_id,
            }))
        consular.register_marathon_event_callback(events_url)

    if sync_interval > 0:
        lc = LoopingCall(consular.sync_apps, purge)
        lc.start(sync_interval, now=True)

    consular.run(host, port, log_file=logfile)
