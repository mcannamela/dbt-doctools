from typing import Iterable, Tuple, Callable, TypeVar, Optional, Union, Protocol, List, Dict, Any
from collections import OrderedDict

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


YamlFragment = Union[str, Dict[str, Any]]
YamlMap = Dict[str, YamlFragment]
YamlList = List[YamlFragment]

class DegenerateMatches(LookupError):
    pass


class NoMatch(LookupError):
    pass


def maybe_get_matching_singleton(elements: Iterable[T], match_it: Callable[[T], bool]) -> Optional[T]:
    matches = [e for e in elements if match_it(e)]
    if len(matches) == 0:
        return
    elif len(matches) == 1:
        return matches[0]
    else:
        raise DegenerateMatches("Multiple matches found when singleton expected")


def unsafe_get_matching_singleton(elements: Iterable[T], match_it: Callable[[T], bool]) -> T:
    match = maybe_get_matching_singleton(elements, match_it)
    if match is None:
        raise NoMatch("No match found")

    return match


def unsafe_get_matching_singleton_by_key(elements: Iterable[Tuple[U, V]], key: U) -> V:
    try:
        return unsafe_get_matching_singleton(elements, lambda t: t[0] == key)
    except NoMatch as exc:
        raise NoMatch(f"No match for '{key}'") from exc
    except DegenerateMatches as exc:
        raise DegenerateMatches(f"Multiple matches for '{key}'") from exc


def apply_to_leaves(yaml: Union[List[Any], Dict[str, Any], Any], f: Callable[[Any], V]):
    if isinstance(yaml, list):
        return [apply_to_leaves(y, f) for y in yaml]
    elif isinstance(yaml, dict):
        return {k: apply_to_leaves(v, f) for k, v in yaml.items()}
    else:
        return f(yaml)


def ordered_fragment(yaml: Union[List[Any], Dict[str, Any], Any], sort_items: Callable[[Tuple[T, U]], V] = None):
    if isinstance(yaml, list):
        return [ordered_fragment(y, sort_items) for y in yaml]
    elif isinstance(yaml, dict):
        return OrderedDict([(k, ordered_fragment(v, sort_items)) for k, v in sorted(yaml.items(), key=sort_items)])
    else:
        return yaml
