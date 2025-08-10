import pytest
import tempfile

from datetime import datetime, timedelta
from pathlib import Path

from libapi.utils.calculations import *


def make_line(date, id, type, fund="HV"):
    return f"{date} - ID: {id} - Type: {type} - Fund: {fund}\n"


@pytest.fixture
def temp_file () :
    """
    
    """
    with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp :
        yield Path(tmp.name)
        tmp.close()
        tmp.unlink()


def test_write_and_read_id (temp_file) :
    """
    
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_to_file(now, "123", "MV", "HV", filename=temp_file)
    
    result = read_id_from_file(now, "MV", "HV", filename=temp_file)
    assert result == "123"


def test_write_duplicate_raises (temp_file) :
    """
    
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    write_to_file(now, "123", "MV", "HV", filename=temp_file)
    
    with pytest.raises(ValueError) :
        write_to_file(now, "123", "MV", "HV", filename=temp_file)


def test_check_duplicate_true (temp_file) :
    """
    
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    temp_file.write_text(make_line(now, "ABC", "MV"))
    
    assert check_duplicate(now, "MV", "HV", filename=temp_file)


def test_check_duplicate_false (temp_file) :
    """
    
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    temp_file.write_text(make_line(now, "XYZ", "Risk"))
    
    assert not check_duplicate(now, "MV", "HV", filename=temp_file)


def test_split_line_parsing () :
    """
    
    """
    line = "2025-07-25 12:00:00 - ID: 001 - Type: MV - Fund: HV"
    date, id_, type_, fund = split_line(line)
    
    assert date == "2025-07-25 12:00:00"
    assert id_ == "001"
    assert type_ == "MV"
    assert fund == "HV"


def test_get_last_run_time (temp_file) :
    """
    
    """
    now = datetime.now()
    earlier = now - timedelta(days=1)
    
    temp_file.write_text(
    
        make_line(earlier.strftime('%Y-%m-%d %H:%M:%S'), "1", "MV") +
        make_line(now.strftime('%Y-%m-%d %H:%M:%S'), "2", "MV")
    
    )
    
    last_time, last_id = get_last_run_time("MV", "HV", filename=temp_file)
    
    assert last_id == "2"
    assert last_time.strftime('%Y-%m-%d %H:%M:%S') == now.strftime('%Y-%m-%d %H:%M:%S')


def test_get_closest_date_of_run_MV (temp_file) :
    """
    
    """
    base = datetime(2025, 1, 1, 12, 0, 0)
    
    dates = [
        base - timedelta(hours=2),
        base - timedelta(hours=1),
        base + timedelta(minutes=30),
        base + timedelta(hours=2)
    ]
    
    ids = ["a", "b", "c", "d"]
    lines = "\n".join(
        [
            make_line(d.strftime('%Y-%m-%d %H:%M:%S'), ids[i], "MV") for i, d in enumerate(dates)
        ]
    )

    temp_file.write_text(lines + "\n")
    closest_date, closest_id = get_closest_date_of_run_mv(base.strftime('%Y-%m-%d %H:%M:%S'), filename=temp_file)
    
    assert closest_id == "c"  # +30min is closest


def test_read_id_time_insensitive (temp_file) :
    """
    
    """
    date_str = "2025-01-01 08:00:00"
    temp_file.write_text(make_line(date_str, "XYZ", "MV"))

    result = read_id_from_file("2025-01-01 15:00:00", "MV", "HV", filename=temp_file, timeSensitive=False)
    
    assert result == "XYZ"