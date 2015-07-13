import click


@click.command()
@click.option('--host', default='localhost')
@click.option('--port', default='7000', type=int)
@click.option('--consul', default='http://localhost:8500',
              help='The Consul HTTP API')
@click.option('--marathon', default='http://localhost:8080',
              help='The Marathon HTTP API')
def main(host, port, consul, marathon):
    from consular.main import Consular
    consular = Consular(consul, marathon)
    consular.app.run(host, port)
