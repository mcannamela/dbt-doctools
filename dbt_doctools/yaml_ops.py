from typing import Iterable, Tuple, Callable, TypeVar, Optional

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


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
        raise NoMatch(f"Multiple matches for '{key}'") from exc