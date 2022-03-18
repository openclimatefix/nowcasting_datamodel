from click.testing import CliRunner

from nowcasting_datamodel.migrations.app import app


def test_app():

    runner = CliRunner()
    response = runner.invoke(app, ["--make-migrations", "--run-migrations"])
    assert response.exit_code == 0, response.exception
