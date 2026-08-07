"""Structural checks on the single-file dashboard.

`web/index.html` carries three views in one script scope and has no build step, so
the two mistakes it is actually exposed to are a duplicated top-level function —
which silently hoists over the earlier one and breaks an unrelated view — and a
`$('id')` whose element was renamed. Both are cheap to detect from the source and
expensive to notice in a browser.
"""

from __future__ import annotations

import re
from pathlib import Path

INDEX = Path(__file__).resolve().parents[2] / "web" / "index.html"
# Declarations at the IIFE's own indentation; anything deeper is a local closure.
TOP_LEVEL_FUNCTION = re.compile(r"^ {4}(?:async )?function ([A-Za-z_$][\w$]*)\s*\(", re.MULTILINE)
DOLLAR_LOOKUP = re.compile(r"\$\('([^']+)'\)")
ELEMENT_ID = re.compile(r'\bid="([^"]+)"')


def _script() -> str:
    text = INDEX.read_text(encoding="utf-8")
    scripts = re.findall(r"<script>(.*?)</script>", text, re.S)
    assert len(scripts) == 1, "the page is expected to carry exactly one script block"
    return scripts[0]


def test_no_top_level_function_is_declared_twice() -> None:
    """A second declaration replaces the first for every caller, including earlier ones."""
    names = TOP_LEVEL_FUNCTION.findall(_script())
    duplicates = sorted({name for name in names if names.count(name) > 1})
    assert not duplicates, f"these functions shadow an earlier definition: {duplicates}"


def test_every_element_lookup_resolves_to_a_declared_id() -> None:
    declared = set(ELEMENT_ID.findall(INDEX.read_text(encoding="utf-8")))
    referenced = set(DOLLAR_LOOKUP.findall(_script()))
    missing = sorted(referenced - declared)
    assert not missing, f"the script looks up ids the document does not define: {missing}"


def test_execution_policy_controls_are_wired_to_request_and_result() -> None:
    text = INDEX.read_text(encoding="utf-8")
    policy = re.search(r'<select[^>]+id="executionPolicy"[^>]*>(.*?)</select>', text, re.S)
    assert policy is not None
    options = policy.group(1)
    assert re.search(r'<option value="adaptive_pov" selected>', options)
    assert '<option value="twap">' in options
    assert '<option value="immediate">' in options
    assert re.search(r'id="executionHorizon"[^>]+min="0\.25"[^>]+max="60"[^>]+value="2"', text)

    script = _script()
    assert "execution_policy: $('executionPolicy').value" in script
    assert "execution_horizon_seconds: value('executionHorizon')" in script
    assert "$('resultPolicy').textContent" in script


def test_successful_backtest_replaces_the_empty_state() -> None:
    script = _script()
    result_renderer = script[script.index("function renderResult(job)") :]
    result_renderer = result_renderer[: result_renderer.index("function renderWarnings")]
    assert "bt.result = result" in result_renderer
    assert "$('btPlaceholder').hidden = true" in result_renderer
    assert "$('btResults').hidden = false" in result_renderer


def test_market_selection_keeps_the_history_route() -> None:
    script = _script()
    selector = script[script.index("async function selectMarket(") :]
    selector = selector[: selector.index("function setupLiveRefresh")]
    assert "historyUrl.hash = '#/history'" in selector
    assert "history.replaceState" in selector


def test_backtest_marks_are_scoped_to_explicit_audit_navigation() -> None:
    script = _script()
    assert (
        "async function selectMarket(slug, preserveSeries = false, preserveTradeMarks = false)"
        in script
    )
    assert "if (!preserveTradeMarks) state.tradeMarks = null" in script
    assert "await selectMarket(slug, false, true)" in script
    assert "state.pendingSlug = null; state.tradeMarks = null" in script
