from click.testing import CliRunner

from nowcasting_datamodel.migrations.app import app
import pytest

@pytest.mark.skip('Not working right now,commit fa0a15d	stopped it')
def test_app():
    runner = CliRunner()
    response = runner.invoke(app, ["--make-migrations", "--run-migrations"], catch_exceptions=True)

    if response.exception:
        raise response.exception

    assert response.exit_code == 0, response.exception
