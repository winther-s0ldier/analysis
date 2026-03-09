# Lessons Learned

## Lesson 1: LLM Agent Runners Need Turn Limits
- **Date:** 2026-03-08
- **Context:** `run_agent_pipeline()` had no turn limit. Models could loop many times.
- **Fix:** Added `max_turns` parameter (default 15, discovery uses 12). Safety valve breaks the loop.
- **Rule:** Every `run_async()` loop MUST have a turn counter and a hard stop.

## Lesson 2: Store Pattern — Never Rely on LLM Text for Structured Data
- **Date:** 2026-03-08
- **Context:** Profiler agent calls `tool_profile_and_classify` correctly, but the final text response may be unreliable. Endpoint tried to parse LLM text → empty data.
- **Fix Pattern:** Store pattern — tools write results to a module-level dict (`_profile_store[session_id] = result`). Endpoints read from the store first, fall back to LLM text parsing.
- **Rule:** NEVER rely on LLM text response to relay structured data. Always have the tool store its own output directly.

## Lesson 3: Fallback Pipeline Must Set Final Status
- **Date:** 2026-03-08
- **Context:** `_fallback_pipeline` returned `{"status": "complete"}` but never set `state.status = "complete"`. Frontend polls `state.status` via `/status` endpoint.
- **Fix:** `_fallback_pipeline` now explicitly sets `state.status = "complete"` at the end.
- **Rule:** The fallback pipeline owns the final state transition.

## Lesson 4: Column Roles Can Be All-None for Generic Datasets
- **Date:** 2026-03-08
- **Context:** For `tabular_generic` datasets, all column_roles are `None`. Library call code builder produced invalid code.
- **Fix:** Added CSV header auto-detection as fallback when column_roles are empty.
- **Rule:** Never assume column_roles has values. Always have a data-driven fallback.

## Lesson 5: Synchronous Code Blocks Event Loop
- **Date:** 2026-03-08
- **Context:** `execute_analysis()` runs heavy pandas computation synchronously inside an async DAG executor. Blocks entire event loop → server unresponsive.
- **Fix:** Wrap `execute_analysis` in `asyncio.to_thread()` so it runs in a background thread.
- **Rule:** Any CPU-heavy or slow I/O operation inside an async function must be offloaded to a thread.

## Lesson 6: Library Code Must Never Trigger LLM Retry
- **Date:** 2026-03-08
- **Context:** When library-generated code fails validation, retry logic called `_get_code_from_coder()` (LLM). Even with max_attempts=1, the LLM call happened inside the loop body before `continue`.
- **Fix:** Added `if code_from_library: break` before any LLM retry call. Library code fails fast.
- **Rule:** Track code origin. Library code gets zero LLM retries. Only LLM-generated code should be retried via LLM.
