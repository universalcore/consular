import click

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
def main(scheme, host, port,
         consul, marathon, registration_id, sync_interval):  # pragma: no cover
    from consular.main import Consular
    from twisted.internet.task import LoopingCall

    consular = Consular(consul, marathon)
    if registration_id:
        events_url = "%s://%s:%s/events?%s" % (
            scheme, host, port,
            urlencode({
                'registration': registration_id,
            }))
        consular.register_marathon_event_callback(events_url)

    if sync_interval > 0:
        lc = LoopingCall(consular.sync_apps)
        lc.start(sync_interval, now=True)

    consular.app.run(host, port)
