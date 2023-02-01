from nowcasting_datamodel.models.convert import convert_list_forecast_value_seven_days_sql_to_list_forecast
from nowcasting_datamodel.save import save_all_forecast_values_seven_days
from nowcasting_datamodel.fake import make_fake_forecasts

from nowcasting_datamodel.models import ForecastValueSevenDaysSQL


def test_convert_list_forecast_value_seven_days_sql_to_list_forecast(db_session):

    # set up
    forecasts = make_fake_forecasts(gsp_ids=range(0, 10), session=db_session)
    db_session.add_all(forecasts)
    db_session.commit()

    save_all_forecast_values_seven_days(session=db_session, forecasts=forecasts)
    forecast_values = db_session.query(ForecastValueSevenDaysSQL).all()

    print('yyyyy')
    print(forecast_values[0].forecast.location.gsp_id)
    print('xxxxxx')

    forecast_new = convert_list_forecast_value_seven_days_sql_to_list_forecast(forecast_values)

    assert len(forecast_new) == 10
