
from total_states_energy import import_data
from tran_total_states_energy import (transform_data, transform_data2)
from query_total_states_energy import (query_data, query_data2)

def test_data_columns_count():
    df = import_data()
    assert len(df.columns) == 7

def test_data_row_count():
    df = import_data()
    assert len(df.index) == 496774
    
def test_transform_data_columns_count():
    df = transform_data()
    assert len(df.columns) == 8

def test_transform_data_row_count():
    df = transform_data()
    assert len(df.index) == 288891
    
def test_transform2_data_columns_count():
    df = transform_data2()
    assert len(df.columns) == 8

def test_transform2_data_row_count():
    df = transform_data2()
    assert len(df.index) == 224662
    
def test_query_data_column_count():
    df = query_data()
    assert len(df.columns) == 3

def test_query_data_row_count():
    df = query_data()
    assert len(df.index) == 52

def test_query_data2_column_count():
    df = query_data2()
    assert len(df.columns) == 3

def test_query_data2_row_count():
    df = query_data2()
    assert len(df.index) == 28
