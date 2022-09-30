
from query_total_energy import query_data
from tran_total_energy import transform_data

def test_transform_data():
    df = transform_data()
    assert len(df.columns) == 6

def test_transform_data_row_count():
    df = transform_data()
    assert len(df.index) == 257
    
def test_query_data_column_count():
    df = query_data()
    assert len(df.columns) == 2

def test_query_data_row_count():
    df = query_data()
    assert len(df.index) == 21
