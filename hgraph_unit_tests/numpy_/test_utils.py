from hgraph import Size, Array
from hgraph.numpy_._utils import extract_dimensions_from_array, extract_type_from_array


def test_extract_type_info():

    assert extract_type_from_array(Array[float, Size[1]]) == float

    assert extract_dimensions_from_array(Array[float, Size[1]]) == (1,)
