
from total_energy import import_data
from transformed_total_energy import transform_data
from query_total_energy import query_data

def test_data_columns_count():
    df = import_data()
    assert len(df.columns) == 7

def test_data_row_count():
    df = import_data()
    assert len(df.index) == 496774
    
def test_transform_data_columns_count():
    df = transform_data()
    assert len(df.columns) != 7

def test_transform_data_row_count():
    df = transform_data()
    assert len(df.index) != 496774
    
def test_query_data_column_count():
    df = query_data()
    assert len(df.columns) == 3

def test_query_data_row_count():
    df = query_data()
    assert len(df.index) == 126
