# Current TODO

## Completed
- [x] Fix profiler store pattern
- [x] Add `max_turns` to `run_agent_pipeline()` — prevents infinite loops
- [x] Add deterministic fallback discovery in `main.py` (`build_fallback_discovery`)
- [x] Add deterministic fallback discovery in `orchestrator.py` (`_fallback_discovery`)
- [x] Pass `max_turns` through `_run_sub_agent_sync` in orchestrator
- [x] Cap discovery agent at 12 turns, synthesis at 12 turns
- [x] Fix `state.dag` not being set in `/discover` endpoint
- [x] Fix `_fallback_pipeline` not setting `state.status = "complete"`
- [x] Fix `_build_library_call_code` crash when column_roles are all None (generic datasets)
- [x] Add CSV header auto-detection as fallback for missing column roles
- [x] Make DAG execution continue with partial results instead of hard-stopping
- [x] Wire `addCustomAnalysis()` to `/add-metric` API + add metric card to discovery stack
- [x] Add `failed` status CSS and `info` badge for custom metric cards
- [x] Fix A8 duplicate execution (spread operator bug)
- [x] Add deterministic synthesis fallback (Stage 4b)
- [x] Add deterministic report generation (Stage 5)
- [x] Vectorize user_segmentation for large datasets
- [x] Library code: skip LLM retry (max_attempts=1, no coder call)
- [x] Run execute_analysis in thread (asyncio.to_thread) to prevent event loop blocking
- [x] Remove Ollama — switch to OpenAI GPT-4o only

## In Progress
- [ ] Test full pipeline end-to-end with OpenAI GPT-4o

## Pending
- [ ] Refactor to lean agentic architecture (coder writes analysis + charts)
- [ ] Test custom analysis UI flow
