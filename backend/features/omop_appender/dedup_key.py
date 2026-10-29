"""
Compact, exact dedup keys.

The appender decides whether a source row is already in the target by building
a key from the table's ``dedup_cols`` and looking it up in a set of the keys
already present in the target. That set can hold millions of entries, and as a
plain tuple of the decoded values each entry costs ~400+ bytes (a tuple plus a
separate Python object for every int, Decimal, date and datetime in it).

``make_key`` instead packs the values into one ``bytes`` object, typically
under 100 bytes, roughly a quarter of the memory.

Exactness is the whole point. Two keys must come out equal **if and only if**
the original tuples would compare equal in Python, so deduplication decisions
are unchanged. Python's ``==`` rules for the types asyncpg returns, and how
each is mirrored here:

  None            equal only to None.                          -> ``_;``
  int/bool/float/ Compared by exact mathematical value across types
  Decimal         (1 == 1.0 == Decimal("1.00") == True, and Decimal("0.1") !=
                  0.1 because the float is binary). Every whole number is
                  written as its integer (``I1500;``), so 0, -0, 0.0,
                  Decimal("-0.00") and Decimal("1.5E+3") vs 1500 line up.
                  Other finite values are written as the str() of the
                  Decimal with trailing zeros stripped (``F1.5;``), computed
                  without rounding; floats go through Decimal(f), which is
                  exact. Infinity keeps its sign. NaN is equal to nothing,
                  so a key holding a NaN becomes a unique never-matching object.
  str             equal to a str with the same characters. -> ``S<len>:<text>``
  date            equal to the same date, never a datetime. -> ``D<iso>;``
  datetime        naive: by its fields, fold ignored.       -> ``T<iso>;``
                  aware with a fixed-offset ``datetime.timezone`` (what
                  asyncpg returns for timestamptz): as a UTC instant
                  -> ``Z<utc iso>;``. Naive and aware are never equal.

Each fragment is self-delimiting (``;``-terminated or length-prefixed), so
joined fragments can't run into each other and different keys can't
collide.

Any value of another type (time, interval, uuid, bytea, arrays, ranges,
aware datetimes with zoneinfo-style tzinfo, extreme numbers, ...) makes
``make_key`` return the plain tuple, the original representation, so its
comparison is exactly as before. None of those types can compare equal to a
value of the types above, so a tuple key and a bytes key never wrongly match.
"""

from datetime import date, datetime, timezone
from decimal import MAX_EMAX, MAX_PREC, MIN_EMIN, Context, Decimal

# Big enough that normalize() never rounds: it only strips trailing zeros.
_CTX = Context(prec=MAX_PREC, Emax=MAX_EMAX, Emin=MIN_EMIN)
_MAX_INT_DIGITS = 4000      # int<->str is capped at 4300 digits by default
_NONE = "_;"
_INF = {True: "N-inf;", False: "N+inf;"}


class _Fallback(Exception):
    """Internal: a value we won't encode; the caller uses the plain tuple."""


def _decimal(d):
    """Fragment for a Decimal, or None for NaN."""
    if not d.is_finite():
        if d.is_nan():
            return None
        return _INF[d.is_signed()]
    if not d:
        return "I0;"
    if d.adjusted() >= _MAX_INT_DIGITS:   # |d| >= 10**4000: same cut-off as ints
        raise _Fallback
    n = d.normalize(_CTX)
    s = str(n)
    # str() of a normalized Decimal uses scientific notation only when the
    # exponent is positive ("1.5E+3", a whole number) or the value is tiny
    # ("1.5E-7"). Otherwise a "." means there are fractional digits left.
    if "E+" in s:
        return f"I{int(n)};"               # whole number, e.g. 1.5E+3 -> 1500
    if "." in s or "E-" in s:
        return "F" + s + ";"              # non-integral
    return "I" + s + ";"                   # plain integer text, e.g. "-15"


def _general(v):
    """Slow path: subclasses, bool, float and everything else."""
    if isinstance(v, str):
        return f"S{len(v)}:{v}"
    if isinstance(v, int):                 # includes bool
        if abs(v) >= 10 ** _MAX_INT_DIGITS:
            raise _Fallback
        return f"I{int(v)};"
    if isinstance(v, float):
        if v != v:
            return None
        if v in (float("inf"), float("-inf")):
            return _INF[v < 0]
        if v.is_integer():
            return f"I{int(v)};"
        return _decimal(Decimal(v))        # exact binary value
    if isinstance(v, Decimal):
        return _decimal(v)
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return "T" + v.replace(fold=0).isoformat() + ";"
        if type(v.tzinfo) is not timezone:
            raise _Fallback
        try:
            u = v.replace(tzinfo=None, fold=0) - v.utcoffset()
        except OverflowError:
            raise _Fallback
        return "Z" + u.isoformat() + ";"
    if isinstance(v, date):
        return "D" + v.isoformat() + ";"
    raise _Fallback


def make_key(values):
    """Build a hashable dedup key from an iterable of column values.

    Returns compact ``bytes`` for the common types, a unique never-matching
    object if any value is NaN, or the plain tuple of the values for any
    other type (see module docstring).
    """
    values = tuple(values)
    parts = []
    append = parts.append
    try:
        for v in values:
            t = type(v)
            # Fast paths for the exact types asyncpg returns most often.
            if t is int and -10**18 < v < 10**18:
                append(f"I{v};")
            elif v is None:
                append(_NONE)
            elif t is str:
                append(f"S{len(v)}:{v}")
            elif t is date:
                append("D" + v.isoformat() + ";")
            elif t is datetime and v.tzinfo is None:
                append("T" + v.isoformat() + ";")
            else:
                p = _decimal(v) if t is Decimal else _general(v)
                if p is None:
                    return object()       # NaN: equal to nothing, as in Python
                append(p)
    except _Fallback:
        return values
    return "".join(parts).encode("utf-8", "surrogatepass")
