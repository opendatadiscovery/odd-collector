from odd_collector.helpers import LowerKeyDict


def test_lower_key_dic():
    test_dict = LowerKeyDict({"FIELD": 1})

    assert "field" in test_dict
    assert test_dict["field"] == 1
    assert "FIELD" not in test_dict
