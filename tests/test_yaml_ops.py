from dbt_doctools.yaml_ops import unsafe_get_matching_singleton_by_key, unsafe_get_matching_singleton, NoMatch, \
    DegenerateMatches
import pytest


@pytest.mark.parametrize(
    ('elements', 'key', 'exp_element'),
    [
        ([(1, 'a')], 1, (1,'a')),
        ([(1, 2), (3, 4), (5, 6)], 5, (5,6)),
        ({'a': 'b', 'c': 'c'}.items(), 'c', ('c', 'c')),
    ]
)
def test_unsafe_get_matching_singleton_by_key(elements, key, exp_element):
    element = unsafe_get_matching_singleton_by_key(elements, key)
    assert exp_element == element


@pytest.mark.parametrize(
    ('elements', 'match_it', 'exp_element'),
    [
        ([1], lambda x: True, 1),
        ([1, 2, 3], lambda x: x == 2, 2),
        (['a', 'b', 'c', 'c'], lambda x: x == 'b', 'b'),
    ]
)
def test_unsafe_get_matching_singleton(elements, match_it, exp_element):
    element = unsafe_get_matching_singleton(elements, match_it)
    assert exp_element == element


@pytest.mark.parametrize(
    ('elements', 'match_it', 'exp_exception'),
    [
        ([], lambda x: True, NoMatch),
        ([1, 2, 3], lambda x: x == 4, NoMatch),
        (['a', 'b', 'c', 'c'], lambda x: x == 'z', NoMatch),
        (['a', 'b', 'c', 'c'], lambda x: x == 'c', DegenerateMatches),
    ]

)
def test_unsafe_get_matching_singleton_raises_if_no_match(elements, match_it, exp_exception):
    with pytest.raises(exp_exception):
        unsafe_get_matching_singleton(elements, match_it)
