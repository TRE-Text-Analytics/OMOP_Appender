"""make_key(a) == make_key(b) must hold exactly when tuple(a) == tuple(b).

Dedup correctness depends on this: a compact key that compared differently
from the original tuple would either let duplicates through or drop genuine
rows. Run from the backend folder (takes ~2 minutes):

    uv run python tests/test_dedup_key.py
"""
import itertools, os, random, sys, uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from features.omop_appender.dedup_key import make_key


def pool():
    """Fresh objects each call, so NaN identity can't leak between sides."""
    tz2 = timezone(timedelta(hours=2)); tzm = timezone(timedelta(hours=-5, minutes=-30))
    return [
        None, 0, 1, -1, True, False, 2**63 - 1, -2**63, 10**30, 1500, 15,
        0.0, -0.0, 1.0, 1.5, 0.1, 1e-30, 1e300, float("inf"), float("-inf"), float("nan"),
        Decimal("0"), Decimal("-0"), Decimal("0.000"), Decimal("1"), Decimal("1.0"), Decimal("1.00"),
        Decimal("1.5"), Decimal("1.50"), Decimal("0.1"), Decimal("1E+3"), Decimal("1500"), Decimal("15E2"),
        Decimal("-1.5"), Decimal("1.500000000000000000000000000000001"), Decimal("1.5000000000000000000000000000000"),
        Decimal("123456789012345678901234567890.123456789"), Decimal("Infinity"), Decimal("-Infinity"),
        Decimal("NaN"), Decimal(0.1), Decimal("1E-30"),
        "", " ", "a", "A", "1", "1.5", "None", "多い", "négatif", "a\x00b", "S1:a", "N+1e0;", "_;",
        date(2020, 1, 1), date(2020, 1, 2), date(1, 1, 1),
        datetime(2020, 1, 1), datetime(2020, 1, 1, 0, 0, 0, 1), datetime(2020, 1, 1, 12, 30),
        datetime(2020, 1, 1, fold=1), datetime(1, 1, 1),
        datetime(2020, 1, 1, tzinfo=timezone.utc), datetime(2020, 1, 1, 2, tzinfo=tz2),
        datetime(2019, 12, 31, 18, 30, tzinfo=tzm), datetime(2020, 1, 1, 12, 30, tzinfo=timezone.utc),
        datetime(1, 1, 1, 1, tzinfo=tz2),
        time(12, 30), timedelta(days=1), uuid.UUID(int=5), b"ab", [1, 2],
        10**18, -10**18, 10**18 - 1, 10**4000, Decimal(10**4000), Decimal("1E+4000"), Decimal("1E+3999"), 10**3999,
        Decimal("15.00"), Decimal("-1.5E+3"), -1500, Decimal("1E-7"), Decimal("0.0000001"), 1e-7, 2.0, Decimal("2"),
        float(2**60), 2**60, Decimal(2**60), 0.5, Decimal("0.5"), Decimal("0.50"), -0.5,
    ]


def check(a, b, stats):
    want = tuple(a) == tuple(b)
    got = make_key(a) == make_key(b)
    stats[0] += 1
    assert want == got, f"MISMATCH: {a!r} vs {b!r}: python={want} key={got}"


stats = [0]
P1, P2 = pool(), pool()
for x, y in itertools.product(range(len(P1)), repeat=2):
    check((P1[x],), (P2[y],), stats)                        # every single-value pair
r2 = random.Random(7)
for _ in range(2_000_000):                                  # 2-column keys incl. mixed-type boundaries
    x1, x2, y1 = (r2.randrange(len(P1)) for _ in range(3))
    y2 = x2 if r2.random() < 0.5 else r2.randrange(len(P1))  # half share a column -> many equal pairs
    check((P1[x1], P1[x2]), (P2[y1], P2[y2]), stats)
# Random realistic 7-column measurement-like keys: dedup must see equal values as equal.
rng = random.Random(1)
for _ in range(1_000_000 // 2):
    d = rng.randint(0, 3)
    val = Decimal(rng.randint(0, 3000)).scaleb(-rng.randint(0, 2))
    a = (rng.randint(1, 50), 3000000 + rng.randint(0, 3), date(2020, 1, 1) + timedelta(d),
         rng.choice([None, datetime(2020, 1, 1) + timedelta(d, minutes=rng.randint(0, 3))]),
         rng.choice([None, val]), rng.choice([None, 0, 4172703]), rng.choice([None, 8840, 8848]))
    b = tuple(Decimal(str(v)) + Decimal("0.00") if isinstance(v, Decimal) and rng.random() < 0.5 else v for v in a)
    check(a, b, stats)
    c = list(a); i = rng.randrange(7); c[i] = rng.choice(P2[:40]); check(a, tuple(c), stats)
print(f"OK: {stats[0]:,} comparisons, make_key agreed with Python equality every time")
