import click


@click.command()
@click.option('--host', default='localhost',
              help='The host to bind to.')
@click.option('--port', default='7000', type=int,
              help='The port to listen to.')
@click.option('--consul', default='http://localhost:8500',
              help='The Consul HTTP API')
@click.option('--marathon', default='http://localhost:8080',
              help='The Marathon HTTP API')
def main(host, port, consul, marathon):
    from consular.main import Consular
    consular = Consular(consul, marathon)
    consular.app.run(host, port)
