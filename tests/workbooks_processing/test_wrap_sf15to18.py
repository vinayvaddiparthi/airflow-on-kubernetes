from workbooks_processing import _wrap_sf15to18


def test_15char_sfdc_id():
    assert _wrap_sf15to18("0010g00001Z54O9") == "0010g00001Z54O9AAJ"


def test_18char_sfdc_id():
    assert _wrap_sf15to18("0010g00001Z54O9AAJ") == "0010g00001Z54O9AAJ"


def test_17char_sfdc_id():
    # Should return None since we're catching the exception and returning that instead.
    assert _wrap_sf15to18("0010g00001Z54O9AA") == None
