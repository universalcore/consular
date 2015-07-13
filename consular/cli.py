import click


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
def main(scheme, host, port, consul, marathon, registration_id):
    from consular.main import Consular
    consular = Consular(consul, marathon,
                        scheme=scheme, host=host, port=port,
                        registration_id=registration_id)
    consular.app.run(host, port)
