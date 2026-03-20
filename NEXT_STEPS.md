# ADK — Next Steps & Improvement Areas

> Last updated: 2026-03-15
> Written for a developer picking up where development left off.
> References actual file paths, function names, and line numbers throughout.

---

## Priority Matrix

| # | Item | Section | Impact | Effort | AWS Critical |
|---|------|---------|--------|--------|--------------|
| 1 | Pipeline timeout watchdog | §5.5 | 🔴 High — stuck pipelines hang forever | Low — ~30 lines in `main.py` | ✅ Yes |
| 2 | Absolute output folder paths | §5.4 | 🔴 High — wrong paths on AWS cause silent failures | Low — surgical grep + fix | ✅ Yes |
| 3 | Redis persistence + multi-worker | §5.1 §5.2 | 🔴 High — horizontal scaling, survives restarts | High — requires Redis infra | ✅ Yes |
| 4 | `ParallelAgent` for DAG nodes | §2 | 🔴 High — runs independent nodes simultaneously vs max 2 | Medium | ❌ No |
| 5 | `exec()` sandbox with resource limits | §5.3 | 🔴 High — no CPU/memory cap on generated code | Medium | ✅ Yes |
| 6 | Coder prompt deduplication | §1.1 | 🟡 Medium — removes redundant tokens from 8+ LLM calls per run | Low — 1 file edit | ❌ No |
| 7 | Synthesis chain-of-thought | §1.3 | 🟡 Medium — more consistent synthesis quality | Medium | ❌ No |
| 8 | Profiler confidence routing | §1.4 | 🟡 Medium — stops bad datasets proceeding blindly | Low | ❌ No |
| 9 | Per-node insight validation | §1.5 | 🟡 Medium — occasional multi-sentence outputs | Low | ❌ No |
| 10 | ADK tracing → SSE | §2 | 🟡 Medium — granular frontend progress (turn-level) | Medium | ❌ No |
| 11 | Chat agent memory | §1.7 | 🟢 Low — better multi-turn chat, zero pipeline speed effect | Low | ❌ No |
| 12 | Confidence intervals on metrics | §4.2 | 🟢 Low — deeper stats, no speed change | High | ❌ No |
| 13 | Hypothesis Agent | §7 | 🟢 Low — additive feature | Medium | ❌ No |
| 14 | Benchmark Agent | §7 | 🟢 Low — additive feature | High | ❌ No |
| 15 | Comparison Agent | §7 | 🟢 Low — additive feature | High | ❌ No |

> **Recommended order for AWS deployment:** 1 → 2 → 5 → 3 → 4

---

## 1. LLM / Prompting Improvements

### 1.1 Coder Agent Prompt Redundancy

The coder system prompt (`agents/coder.py` lines 33–156) lists the full library function reference **twice** — once in the "AVAILABLE LIBRARY FUNCTIONS" block and again in the "FULL LIBRARY FUNCTION REFERENCE" block. These are identical. The duplication wastes tokens on every node invocation and should be collapsed to one block.

Additionally, the prompt hardcodes the function-to-args mapping as raw text. As the library grows, this will drift from `LIBRARY_REGISTRY`. A better approach: generate the function reference dynamically at agent construction time from `LIBRARY_REGISTRY` so it is always accurate.

### 1.2 Discovery Agent Over-instruction

The discovery system prompt (`agents/discovery.py` lines 255–349) lists all analysis types with `ALIAS_MAP` warnings (e.g., "DO NOT use `path_analysis`"). These notes exist because the LLM keeps generating deprecated type names. The cleaner fix is to remove alias entries from `LIBRARY_REGISTRY` entirely and let `tool_submit_analysis_plan()` silently remap without needing to warn the LLM in the prompt.

### 1.3 Synthesis Prompt Chain-of-Thought

The synthesis instruction tells the agent to "reason step-by-step before writing" (prompt injected by `build_synthesis_prompt()`, `orchestrator.py` line 166), but this reasoning is not structured. The agent sometimes skips the reasoning phase and jumps straight to the output. A more reliable approach: add an explicit "reasoning scratchpad" section with required sub-questions (e.g., "What is the most surprising finding?", "Which two nodes most directly interact?") before the synthesis JSON is generated. Chain-of-thought structured as a separate tool call would be the cleanest implementation.

### 1.4 Profiler Confidence Score Not Used Downstream

The Profiler Agent outputs a `confidence` float (0.0–1.0) in the classification JSON (`agents/profiler.py` line 87). This is never read by the orchestrator or discovery agent. A low confidence score (< 0.7) should trigger the Discovery Agent to ask more conservative feasibility checks — or prompt the user to confirm column roles before proceeding.

### 1.5 Per-Node Insight Quality

The per-node decision-maker insight (#10, `orchestrator.py` line 992–1031) uses a raw `google.genai.Client().models.generate_content()` call with a one-sentence constraint. The constraint is sometimes violated — the LLM returns multiple sentences or a generic opener like "The analysis shows...". The constraint is in the prompt but needs a post-processing filter (first sentence extraction, or a simple regex check before storing) to be reliable.

### 1.6 Critic Agent Rarely Changes Anything

The Critic Agent (`agents/critic.py`) runs but its `confidence_adjustment` and `challenges` are stored in `_critic_review` inside the synthesis store but are **never surfaced to the user** in a meaningful way in the current HTML report. The `_build_report_html()` function in `dag_builder.py` has a comment referencing `_critic_review` but the rendering is minimal. Consider rendering a "Reliability Badge" in the report header that shows the confidence adjustment and the top 2 challenges as callout blocks.

### 1.7 Chat Agent Has No Memory

The Chat Agent (`agents/chat_agent.py`) is stateless. Each message re-injects the full context. For multi-turn chat, the conversation history is not included in subsequent prompts — the agent cannot reference its own previous answer. The ADK `InMemorySessionService` maintains session state but the current implementation only passes the full context once per turn, not the conversation thread. The session should persist across `/chat` calls so the agent can refer back to what was discussed.

---

## 2. ADK Feature Improvements

| Feature | Current State | What to Do | File / Line |
|---|---|---|---|
| `ParallelAgent` | Not used. DAG nodes run with a manual asyncio Semaphore (concurrency=2). | Replace the semaphore approach with a `google.adk.agents.ParallelAgent` wrapping the independent root-level DAG nodes. This is what `ParallelAgent` is designed for. | `orchestrator.py` line 800 |
| Agent memory / `ToolContext` | Not used. Agents are stateless beyond the single prompt. | The ADK `ToolContext` exposes `state` for persistent tool-level memory. The per-node insight (#10) currently uses a raw `genai.Client()` call outside ADK entirely. Moving it inside an ADK tool would give it proper session tracking and retry semantics. | `orchestrator.py` line 998 |
| Tool schemas | ADK supports typed tool schemas via Pydantic models. All current tools use plain Python signatures with no type validation or description fields beyond docstrings. | Add Pydantic models to the complex tool signatures (especially `tool_submit_synthesis`, `tool_submit_critique`, `tool_submit_analysis_plan`). This improves ADK's ability to auto-generate the function calling schema the LLM sees. | `agents/synthesis.py` line 442, `agents/critic.py` line 89 |
| `SequentialAgent` | Not used. The pipeline stages are implemented as sequential `await run_agent_pipeline()` calls. | Stages 1–3 (Profile → Discover → DAG) could be expressed as a `SequentialAgent`, making the pipeline topology explicit and observable in ADK's built-in tracing. | `orchestrator.py` line 239 |
| ADK tracing / event stream | Not used. All observability is via `print()` + `pipeline.log`. | The ADK runner emits events per turn. These could be forwarded to the SSE stream for more granular frontend updates (e.g., showing "Coder Agent turn 3/15" instead of the fake rotating status messages). | `main.py` line 231 (runner loop) |
| `google.adk.sessions` persistence | `InMemorySessionService` is used — sessions are lost on restart. | For a deployed version, swap to a `DatabaseSessionService` (PostgreSQL or Redis) so in-progress pipelines survive server restarts. The `SessionState` in `main.py` is separate from the ADK session and would also need persisting. | `main.py` line 64 |
| Agent instance caching | All agents use module-level singleton pattern (`_coder_agent_instance = None`). This is fine for single-process but breaks under multi-worker uvicorn. | Either pin to single worker (`--workers 1`) or switch to per-request agent instantiation with a lightweight factory that doesn't cache globally. | All `agents/*.py` |

---

## 3. A2A Protocol Improvements

The A2A message protocol (`a2a_messages.py`) is fully implemented and agents post messages at each stage transition. However, the messages are largely **write-only** — they are recorded in `state.message_log` but are never read back to drive pipeline decisions.

| Gap | Detail | Fix |
|---|---|---|
| Dependency resolution uses `PipelineState`, not messages | `PipelineState.get_ready_to_run()` checks node status directly. `is_dependency_resolved()` in `a2a_messages.py` exists but is never called. | Either unify dependency resolution to use messages (the intended A2A pattern) or remove the dead utilities to reduce confusion. |
| `CLARIFICATION_NEEDED` intent is defined but never fired | Defined at line 34 of `a2a_messages.py`. No agent ever generates this intent. | The Discovery Agent should emit this intent if it detects ambiguous column roles (e.g., two candidate entity columns) and the frontend should handle it by showing a clarification UI before proceeding. |
| `INSTALL_REQUIRED` / `INSTALL_COMPLETE` intents are dead | Defined in `a2a_messages.py` but no code posts or reads these. | Remove them or implement a package-install agent if custom analysis library functions have optional dependencies (scipy, scikit-learn, etc.) that may not be installed. |
| A2A messages have no timestamps used for ordering | `A2AMessage.timestamp` is set but `get_messages_by_intent()` returns all matching messages in insertion order without time-based sorting. | Use timestamps to support `get_latest_message()` correctly when messages are delivered out of order (which can happen with async concurrent nodes). |
| No cross-agent A2A routing | All messages go into a single flat `state.message_log`. There is no routing — agents that want messages must scan the full log. | If the message log grows large (many nodes, retries), this becomes O(n) per query. Add an indexed mailbox per recipient (already partially scaffolded in `SessionState.get_messages_for()`). |

---

## 4. Analysis Depth

### 4.1 Shallow Analysis Types

| Analysis Type | Shallow Area | What to Add |
|---|---|---|
| `distribution_analysis` | Produces basic histogram + box plot metrics. `is_normal` flag is set but not used in synthesis to modulate which statistical tests are valid. | Add Shapiro-Wilk test for small samples. Add QQ-plot data. Surface the `is_normal` flag as a warning when downstream analyses assume normality (e.g., Pearson correlation on non-normal columns). |
| `correlation_matrix` | Reports Pearson + Spearman but does not handle multicollinearity (VIF). | Add VIF computation for datasets with many numeric columns; flag columns that inflate each other's apparent importance. |
| `cohort_analysis` | Groups by first-seen date but has no configurable cohort window (day/week/month). The window is inferred. | Allow the Discovery Agent to set `cohort_window` in the column_roles spec. |
| `event_taxonomy` | Uses keyword matching to assign events to 9 categories. Categories are hardcoded. | Replace with an LLM call that classifies event names into user-defined or domain-appropriate categories, using the `event_col`'s sample values from the profiler. |
| `user_segmentation` | DBSCAN clustering on event frequency vectors. Does not explain clusters — just reports sizes and characteristics. | Run a follow-up LLM call (like the #10 per-node insight) that interprets each cluster's characteristics in plain English and names the segment. |
| `sequential_pattern_mining` | PrefixSpan finds frequent sequences but does not associate sequences with outcomes. | Add an outcome correlation step: for sessions containing sequence X, what is their average outcome / completion rate? This converts pattern mining into actionable intelligence. |
| `survival_analysis` | Kaplan-Meier curve only. No log-rank test between groups. | Add group comparison: split by a categorical column (if available) and test whether survival differs significantly between groups using the log-rank test. |

### 4.2 Missing Statistical Depth

- **No confidence intervals** on any metric except `confidence` (which is agent-estimated, not statistically computed). Point estimates without CIs are misleading for decision-making.
- **No multiple comparison correction**: when `correlation_matrix` returns many "significant" correlations, no Bonferroni or FDR correction is applied. False positive rate is high.
- **No causal inference**: the system identifies correlations and builds causal hypotheses in synthesis, but no actual causal analysis (e.g., propensity score matching, difference-in-differences) is available even when the data would support it.
- **No significance testing for trend changepoints**: `run_trend_analysis()` detects changepoints but does not test whether the before/after difference is statistically significant.

---

## 5. Robustness on AWS Deployment

### 5.1 In-Memory State Lost on Restart

All session state, synthesis results, pipeline state, and SSE event queues are in Python dicts in process memory. An EC2 instance restart, uvicorn crash, or ALB health-check-induced restart loses all in-progress sessions. The user sees a "Session not found" error.

**Fix:** Persist `SessionState` to Redis (session data) and write synthesis/results to S3 after each node completes. The `_synthesis_cache.json` file backup (`agents/synthesis.py` line 606) is a partial solution for synthesis only — extend this pattern to node results.

### 5.2 Single-Worker Constraint

Module-level singleton agent instances (`_coder_agent_instance`, `_synthesis_agent_instance`, etc.) are not safe for multiple uvicorn workers. Running with `--workers > 1` would cause each worker to have its own `sessions` dict and module-level stores, making cross-request state invisible.

**Fix:** Either enforce `--workers 1` (current safe configuration) with documentation, or move all mutable state to Redis/DynamoDB and make agent instances stateless.

### 5.3 `exec()` Security

`code_executor.py` uses Python `exec()` to run coder-agent-generated code in-process. The safety check (`validate_code()` lines 130–143) blocks `os.remove`, `subprocess`, `eval`, `exec`, etc., but does NOT sandbox:
- Network calls (`requests`, `urllib`, `socket`)
- File reads outside the upload directory
- CPU-intensive infinite loops (no timeout)
- Memory exhaustion via large `pd.read_csv()` without row limits

**Fix:** Move code execution to a subprocess with `resource.setrlimit()` for CPU/memory limits, or use a container-based sandbox (e.g., AWS Lambda for code execution).

### 5.4 Output Folder Path Assumptions

`state.output_folder` stores a relative folder name (e.g., `"Commuter_Users_Event_data"`). The absolute path is computed by joining with `OUTPUT_DIR` in `main.py`. Several places (notably `agents/synthesis.py` line 607 and `agents/dag_builder.py` line 22) recompute the absolute path using `os.path.join(adk_root, "output", _out)`. If the working directory differs from the ADK root (common on AWS with process supervisors), these joins produce wrong paths.

**Fix:** Store the absolute output folder path on `state.output_folder` at creation time (`main.py` line 372) and pass it consistently. Do not recompute from `__file__`.

### 5.5 No Request Timeout on `/analyze`

`POST /analyze/{session_id}` starts a background thread and returns immediately with `{"status": "started"}`. There is no mechanism to enforce a maximum pipeline execution time. If a pipeline hangs (e.g., stuck LLM call), the session stays in `analyzing` state forever and the SSE stream never sends `stream_end`.

**Fix:** Add a background watchdog that marks the session as `error` and pushes a `stream_end` SSE event if `state.status` has been `analyzing` for more than a configurable timeout (e.g., 15 minutes).

### 5.6 Chart PNG Generation May Fail Silently

The synthesis prompt builder (`orchestrator.py` line 177–184) looks for PNG files alongside HTML charts (`cp.replace(".html", ".png")`). PNGs are only generated if `kaleido` is installed (Plotly's static image export library). If kaleido is absent (common on AWS), no images are passed to the multimodal synthesis call, and chart visual interpretation in synthesis is lost silently.

**Fix:** Add a startup check for `kaleido`; log a clear warning; consider falling back to passing the chart HTML data structure as text for synthesis.

---

## 6. UI/UX Improvements

| Area | Current State | Improvement |
|---|---|---|
| **Report embedded in chat** | The final report HTML is fetched via `/api/session/{id}/report` and rendered as an innerHTML in the chat area. This is fragile — large Plotly charts can lag the page, and CSP headers may block inline scripts. | Serve the report in a dedicated tab or iframe with `sandbox` attribute, or open it in a new tab via `/output/{folder}/report.html` served as a static file. |
| **No synthesis quality indicator** | The synthesis quality guard runs but the user never sees whether it passed or failed, or how many retries were needed. | Show a small badge ("Synthesis: 2 retries, QC passed") in the report header or chat message. |
| **Critic review not prominent** | The critic's `approved`, `challenges`, and `confidence_adjustment` are stored in `_critic_review` but the current report HTML renders them minimally. | Add a "Peer Review" section at the top of the report with the overall verdict, confidence badge (e.g., "92% reliable"), and a collapsible list of challenges. |
| **No metric approval UI** | The `/discover` response includes the full DAG node list, but the frontend renders it as a plain message without checkboxes. The "approved_metrics" parameter in `/analyze` exists and is supported. | Add a "Choose analyses" step between Discover and Analyze with checkboxes per node, plus an "Add custom metric" text input that calls `/validate-metric/{session_id}`. |
| **No user instructions hint** | The chat input shows "Add instructions (optional)" after attaching a file, but there is no hint about what kinds of instructions are useful. | Add a placeholder suggestion like "e.g. Focus on checkout drop-off" or show example prompts as chip buttons. |
| **No download button for report** | The report is rendered inline. Users cannot easily download it. | Add a "Download Report" button that triggers `GET /api/session/{id}/report` and `Content-Disposition: attachment`. |
| **Terminal card does not show node types** | The terminal card shows `node_id` but not `analysis_type`. For users unfamiliar with IDs like "A3", this is opaque. | Show the analysis type alongside the ID: `A3 · funnel_analysis ✓`. |
| **No error detail on failure** | When a node fails, the terminal card shows "failed" but no error message is visible. | Show a hoverable error tooltip or expandable error detail per failed node, pulling from `/api/session/{id}/status`'s `node_statuses` extended with `error_message` fields. |
| **Lenis smooth scroll** | Loaded from CDN (`Lenis` global check at `app.js` line 224). If CDN is unavailable, scroll falls back to native with no error. | Bundle Lenis locally or add a visible fallback class. |
| **No mobile layout** | `style.css` has no responsive breakpoints for the sidebar. On narrow screens the sidebar overlaps content. | Add a hamburger menu for mobile with the sidebar as an overlay drawer. |

---

## 7. New Agent Ideas

| Agent Name | What It Would Do | Why Valuable | Implementation Notes |
|---|---|---|---|
| **Hypothesis Agent** | After synthesis, generates 3–5 testable hypotheses from the findings (e.g., "If we add a confirmation screen before step X, funnel completion will increase by Y%"). Each hypothesis includes: the metric to measure, expected effect direction, and a minimum detectable effect size. | Turns synthesis insights into an actionable experiment backlog. Currently the synthesis "how_to_fix" items are prescriptive but not testable. | Runs after Critic. Reads `_synthesis_store`. No tools — pure reasoning agent. Output stored in synthesis as `hypotheses[]`. |
| **Data Repair Agent** | Receives gate warnings about high null density, mixed types, or duplicate rows and proposes + optionally applies a repair strategy (e.g., imputation, deduplication). | The current gate only warns. Users with messy data get blocked or get degraded analysis. | Would need `execute_repair(csv_path, strategy)` tool with strict sandboxing. Could output a `repaired.csv` alongside the original. |
| **Schema Mapper Agent** | When schema drift is detected (new/renamed columns in `schema_registry.json`), this agent reasons about whether the new columns are semantically equivalent to old ones and updates the registry. | Currently schema drift emits a warning but no action is taken. Repeated uploads of evolving datasets accumulate warnings. | Reads the old and new schema fingerprints from `data_gate.py`. Calls the LLM to reason about column renames. Writes back to `data/schema_registry.json`. |
| **Benchmark Agent** | After synthesis, looks up industry benchmarks for the key metrics found (e.g., "average e-commerce funnel conversion is 2.5%") and contextualizes the dataset's results against them. | Users frequently ask "is this good?" The synthesis is explicitly forbidden from inventing benchmarks but users need context. | Would need a tool that queries a curated benchmark database (could start as a hardcoded dict for common metric types, expandable via web search). Must clearly label benchmarks as external reference, not data-derived. |
| **Incremental Analysis Agent** | When a new version of the same dataset is uploaded (detected by schema_registry match), runs only the analyses where the results have changed significantly (e.g., drift > 2 standard deviations from previous run). | Repeated uploads of daily/weekly refreshed data currently re-run the full pipeline. This is expensive and slow. | Requires storing previous results per dataset type in persistent storage. Delta detection logic in `data_gate.py` or a new `drift_detector.py`. |
| **Narrative Localizer Agent** | Takes the completed `conversational_report` and rewrites it for a specific audience or locale (e.g., "rewrite for a non-technical stakeholder in French" or "rewrite for an engineering team"). | Synthesis output is currently one-size-fits-all English. Different stakeholders need different framing. | Reads `_synthesis_store[session_id]["conversational_report"]`. Single-call rewrite agent. Output added as an alternate report section. No tools needed. |
| **Comparison Agent** | Given two session IDs (two different datasets or two different time windows of the same dataset), produces a structured delta report: which metrics improved, which degraded, and what the most significant shift was. | Currently there is no cross-session comparison. Users who upload "January data" then "February data" have no way to compare. | Reads `state.results` from two sessions. Needs a new `/compare` endpoint. The agent reasons about metric-by-metric changes. |
