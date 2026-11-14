"""Prints quick_relevance_score for 12 sample headlines (6 housing, 6 unrelated).

Run from the backend folder:  python tests/quick_score_check.py

Exit code 0 means every housing headline cleared the filter threshold and every
unrelated headline did not. Exit code 1 means at least one headline was wrong.
"""
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from ai_processor import quick_relevance_score  # noqa: E402


def _filter_threshold() -> int:
    """Read QUICK_FILTER_THRESHOLD from scraper.py without importing its heavy deps."""
    try:
        with open(os.path.join(ROOT, "scraper.py"), encoding="utf-8") as fh:
            m = re.search(r"^QUICK_FILTER_THRESHOLD\s*=\s*(\d+)", fh.read(), re.M)
        if m:
            return int(m.group(1))
    except OSError:
        pass
    return 0


# scraper.py drops an article when score <= QUICK_FILTER_THRESHOLD.
THRESHOLD = _filter_threshold()

HEADLINES = [
    ("Province announces new supportive housing sites in Surrey", True),
    ("Encampment response plan approved by Vancouver council", True),
    ("Reaching Home funding extended for Indigenous homelessness projects", True),
    ("Auditor General releases review of shelter operator oversight", True),
    ("Toxic drug deaths decline in BC, coroners report says", True),
    ("Rent supplement program expands eligibility", True),
    ("Canucks win in overtime", False),
    ("Celebrity chef opens new restaurant downtown", False),
    ("Weather forecast: rain returns this weekend", False),
    ("Bitcoin price tops new high", False),
    ("Box office weekend results", False),
    ("Recipe: easy weeknight pasta", False),
]


def main() -> int:
    print(f"Filter rule: an article is dropped when score <= {THRESHOLD}\n")
    print(f"{'RESULT':<7} {'SCORE':>5}  {'EXPECTED':<9} HEADLINE")
    print("-" * 100)
    failures = 0
    for headline, should_pass in HEADLINES:
        score = quick_relevance_score(headline, "")
        passes = score > THRESHOLD
        ok = passes == should_pass
        failures += 0 if ok else 1
        expected = "pass" if should_pass else "filtered"
        print(f"{'OK' if ok else 'WRONG':<7} {score:>5}  {expected:<9} {headline}")
    print()
    print("ALL HEADLINES BEHAVE AS EXPECTED" if failures == 0 else f"{failures} HEADLINE(S) WRONG")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
