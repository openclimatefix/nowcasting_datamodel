from datetime import datetime, timezone, timedelta

from freezegun import freeze_time

from nowcasting_datamodel.read.blend.weights import make_weights_df


@freeze_time("2023-01-02 00:00:01")
def test_make_weights_df_yesterday():
    start_datetime = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    weights = make_weights_df(
        model_names=["test_1", "test_2"], weights=None, start_datetime=start_datetime
    )
    assert len(weights) == 49 + 17
    assert "test_1" in weights.columns
    assert "test_2" in weights.columns

    assert weights.index[0].isoformat() == start_datetime.isoformat()
    assert (
        weights.index[15].isoformat()
        == (start_datetime + timedelta(hours=7, minutes=30)).isoformat()
    )

    assert weights["test_1"][0] == 1
    assert weights["test_2"][0] == 0

    assert weights["test_1"][49] == 1
    assert weights["test_2"][49] == 0

    assert weights["test_1"][49 + 3] == 1
    assert weights["test_2"][49 + 3] == 0

    assert weights["test_1"][49 + 7] == 0.5
    assert weights["test_2"][49 + 7] == 0.5

    assert weights["test_1"][49 + 11] == 0
    assert weights["test_2"][49 + 11] == 1

    assert weights["test_1"][49 + 15] == 0
    assert weights["test_2"][49 + 15] == 1


@freeze_time("2023-01-01 00:00:01")
def test_make_weights_df():
    start_datetime = datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc)

    weights = make_weights_df(model_names=["test_1", "test_2"], weights=None)
    assert len(weights) == 17
    assert "test_1" in weights.columns
    assert "test_2" in weights.columns

    assert weights.index[0].isoformat() == start_datetime.isoformat()
    assert (
        weights.index[15].isoformat()
        == (start_datetime + timedelta(hours=7, minutes=30)).isoformat()
    )

    assert weights["test_1"][0] == 1
    assert weights["test_2"][0] == 0

    assert weights["test_1"][3] == 1
    assert weights["test_2"][3] == 0

    assert weights["test_1"][7] == 0.5
    assert weights["test_2"][7] == 0.5

    assert weights["test_1"][11] == 0
    assert weights["test_2"][11] == 1

    assert weights["test_1"][15] == 0
    assert weights["test_2"][15] == 1


@freeze_time("2023-01-01 00:00:01")
def test_make_weights_df_forecast_horizon():
    start_datetime = datetime(2023, 1, 1, 0, 30, tzinfo=timezone.utc)

    weights = make_weights_df(
        model_names=["test_1", "test_2"], weights=None, forecast_horizon_minutes=60 * 3
    )
    assert len(weights) == 17
    assert "test_1" in weights.columns
    assert "test_2" in weights.columns

    assert weights.index[0].isoformat() == start_datetime.isoformat()
    assert (
        weights.index[15].isoformat()
        == (start_datetime + timedelta(hours=7, minutes=30)).isoformat()
    )

    assert weights["test_1"][0] == 0.75
    assert weights["test_2"][0] == 0.25

    assert weights["test_1"][15] == 0.75
    assert weights["test_2"][15] == 0.25

    assert (weights["test_1"] == 0.75).all()
    assert (weights["test_2"] == 0.25).all()

    weights = make_weights_df(
        model_names=["test_1", "test_2"], weights=None, forecast_horizon_minutes=-1
    )
    assert (weights["test_1"] == 1.0).all()
    assert (weights["test_2"] == 0.0).all()

    weights = make_weights_df(
        model_names=["test_1", "test_2"], weights=None, forecast_horizon_minutes=10000
    )
    assert (weights["test_1"] == 0.0).all()
    assert (weights["test_2"] == 1.0).all()
