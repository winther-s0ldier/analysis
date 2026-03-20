# ADK — Agentic Analytics: Architecture Reference

> Last updated: 2026-03-15
> Covers: all agents, tools, data flow, API surface, and recent feature additions.

---

## 1. System Overview

ADK is a multi-agent analytics pipeline that takes any structured tabular dataset (CSV, Excel, JSON, Parquet) and autonomously produces a full business intelligence report without the user needing to write code or choose analysis methods.

**Who uses it:** Data analysts, product managers, and operations teams who want deep quantitative insight from event logs, transaction tables, or survey data without managing an analytics stack.

**What problem it solves:** Traditional BI requires a human analyst to decide which analyses to run, write the code, interpret the results, and write the narrative. ADK replaces the entire pipeline with a cascade of specialized LLM agents that profile the data, plan the analyses, write and execute the code, synthesize findings into a narrative, critique that narrative for factual accuracy, and render a final HTML report — all triggered by a single file upload.

**How it is consumed:** A browser-based chat interface. The user uploads a file (drag-and-drop or attach), optionally types a focus question, and watches the pipeline execute in real time via a terminal card in the chat and a progress ring. At the end, the report loads inline.

---

## 2. Tech Stack

| Layer | Technology |
|---|---|
| Web server | FastAPI (Python) with `uvicorn` |
| LLM runtime | Google ADK (`google.adk`) — `Runner`, `InMemorySessionService`, `Agent`, `LoopAgent` |
| LLM model | Google Gemini (`gemini-3.1-pro-preview` default, controlled via `GEMINI_MODEL` env var) |
| Agent-to-agent | Custom A2A message protocol (`a2a_messages.py`) over in-memory `SessionState.message_log` |
| Real-time updates | Server-Sent Events (SSE) via `/stream/{session_id}`, polling fallback |
| Analysis code | Python/pandas/numpy/scipy executed in-process via `exec()` sandbox |
| Charts | Plotly (HTML files embedded in report) |
| Frontend | Vanilla JS + HTML/CSS (no framework) — `static/app.js`, `static/style.css`, `static/index.html` |
| Scrolling | Lenis smooth-scroll library (CDN) |
| File formats | CSV, XLSX, XLS, JSON, JSONL, Parquet — normalized to CSV before pipeline |
| Persistence | In-memory only (sessions dict, module-level stores) — no database |
| Logging | `pipeline.log` file (Python `logging`) + stdout color-coded monitor |

---

## 3. File Map

### Root

| File | Description |
|---|---|
| `main.py` | FastAPI application entry point. Defines all HTTP endpoints, `SessionState` class, SSE event store, `run_agent_pipeline()` runner, and `extract_json()` helper. Orchestrates background pipeline task. |
| `a2a_messages.py` | Agent-to-agent message protocol. Defines `A2AMessage`, `Intent`, `NodeStatus`, `MetricSpec`, `AnalysisResult`, `PipelineState` dataclasses. All agents import from here. |

### `agents/`

| File | Description |
|---|---|
| `orchestrator.py` | Pipeline coordinator. Contains `run_full_pipeline()` (stages 1–5), `_execute_dag()` (async DAG runner with semaphore concurrency=2), `_execute_single_node()` (per-node code gen + exec), `build_synthesis_prompt()` (shared by pipeline and `/rerun-synthesis`). Also hosts the `_pipeline_store`, `_pipeline_event_hooks`, and `_global_threads` dicts. |
| `profiler.py` | Profiler Agent. Calls `tool_profile_and_classify()` to get raw column stats, then uses the LLM to assign column roles (`entity_col`, `time_col`, `event_col`, `outcome_col`, `funnel_col`) and classify the dataset type. Outputs structured JSON with `raw_profile` + `classification`. |
| `discovery.py` | Discovery Agent. Receives profiler output and designs a dependency-correct analysis DAG (4–9 nodes). Contains `tool_submit_analysis_plan()` which validates, alias-remaps analysis types, and stores the plan; and `build_dag_deterministic()` as a fallback if the LLM fails. |
| `coder.py` | Coder Agent. Pure code-writer — no tools. Given an analysis spec, writes a `def analyze(csv_path: str) -> dict` Python function that either calls a registered library function or implements custom pandas/numpy logic. |
| `synthesis.py` | Synthesis Agent. Calls `tool_aggregate_results()` to collect all node results and fact sheets, then writes the full intelligence report (executive summary, detailed insights, cross-metric connections, personas, intervention strategies, conversational report). Contains `tool_submit_synthesis()` with a multi-stage quality guard. Also hosts `_synthesis_store` and `_reasoning_store`. |
| `critic.py` | Adversarial Critic Agent (#8). Reviews the synthesis for unsupported claims, contradictions, overconfidence, missed critical findings, and vague action items. Outputs `approved` flag + `challenges` list + `confidence_adjustment`. Stores result in `_critic_store`. |
| `dag_builder.py` | Report Builder Agent. Calls `tool_build_report()` which collects all chart HTML files, embeds them alongside synthesis sections, and renders the final `report.html`. Also writes `synthesis.json` to the output folder. |
| `chat_agent.py` | Chat Agent. Tool-less conversational agent. Answers post-pipeline questions using full context (synthesis, all findings, column profile) injected directly into the prompt. Pre-pipeline, chat messages are stored as `user_instructions` for the next run. |

### `tools/`

| File | Description |
|---|---|
| `analysis_library.py` | Pre-built, tested analysis functions. Defines `LIBRARY_REGISTRY` (dict of 26 analysis types → function metadata) and implements all analysis functions that return a standardized result envelope. |
| `code_executor.py` | Code sandbox utilities. `validate_code()` (syntax + safety check + dry run), `execute_analysis()` (exec + chart generation), `validate_output_quality()` (structural result checks), `submit_result()` (stores to `_result_store`). No LLM. |
| `model_config.py` | Central model registry. `get_model(agent_name)` returns the model string. All agents import from here. Default: `gemini-3.1-pro-preview` (overridden by `GEMINI_MODEL` env var). |
| `csv_profiler.py` | Raw CSV statistics. `profile_csv()` computes column types, cardinality, nulls, correlations, sample rows. Called by `tool_profile_and_classify()` before the LLM sees the data. |
| `chart_generator.py` | Plotly chart rendering. Converts `chart_ready_data` dicts (produced by analysis functions) into Plotly HTML files. Called by `execute_analysis()`. |
| `ingestion_normalizer.py` | Multi-format ingestion. Converts CSV/XLSX/XLS/JSON/JSONL/Parquet to a clean UTF-8 CSV. Handles BOM stripping, encoding detection, Excel junk rows, nested JSON flattening. |
| `data_gate.py` | Pre-flight data quality gate. Checks file sanity, null density (block at >90%), duplicate rows (warn at >20%), type consistency, and schema drift against `data/schema_registry.json`. Returns `gate_result: pass | warn | block`. |
| `monitor.py` | Lightweight in-process event bus. `emit()` logs events to `_events_store[session_id]`. `check_failure_threshold()` fires a `HIGH_FAILURE_RATE` alert if >30% of DAG nodes fail. |
| `data_policy.py` | Policy system for controlling which analyses run. `get_active_policy()` reads a policy config; `build_policy_context_for_discovery()` injects constraints into the discovery prompt; `apply_policy_to_dag()` filters/caps the final DAG. |
| `workflow_loader.py` | Custom workflow profiles per dataset type. `register_custom_analyses()` injects external analysis functions into the registry; `get_dataset_profile()` returns dataset-type-specific overrides (force_analyses, exclude_analyses, max_nodes). |
| `synthesis_helpers.py` | Shared synthesis utilities. |
| `file_manager.py` | Output folder and file path management. |

### `static/`

| File | Description |
|---|---|
| `index.html` | Single-page application shell. Sidebar with 6-stage pipeline nav, chat scroll area, file attach UI, progress ring, top progress bar. Loads `style.css` and `app.js`. |
| `app.js` | All frontend logic (~900+ lines). Upload flow, SSE stream handler, polling fallback, terminal card, chart rendering, sidebar stage management, chat send/receive, progress ring updates. |
| `style.css` | Dark theme, sidebar, chat bubbles, terminal card, progress ring, chart embed styling. |

### `data/`

| File | Description |
|---|---|
| `schema_registry.json` | Persistent schema fingerprints for drift detection. Keyed by dataset_type; each entry has `columns` fingerprint hash and `last_seen` timestamp. Written by `register_schema()` after each successful pipeline run. |

---

## 4. Pipeline Flow

The complete pipeline is driven by `run_full_pipeline()` in `agents/orchestrator.py` (line 189), triggered as a background thread from the `/analyze/{session_id}` endpoint.

### Stage 0 — Upload & Normalize (HTTP, synchronous)

1. User uploads a file via `POST /upload`.
2. `ingestion_normalizer.normalize_file()` converts the file to clean UTF-8 CSV.
3. `data_gate.run_preflight_check()` validates the CSV (row count, null density, duplicates, schema drift).
4. If gate result is `block`, the upload is rejected with HTTP 422.
5. A `SessionState` object is created and stored in `sessions[session_id]`.
6. Session ID returned to frontend.

### Stage 0.5 — Profile & Discover (HTTP, synchronous, user-visible)

Before `/analyze`, the frontend sequentially calls:
- `POST /profile/{session_id}` — runs Profiler Agent, stores `raw_profile` + `semantic_map` + `dataset_type` on state.
- `POST /discover/{session_id}` — runs Discovery Agent, stores `dag` on state.

These two stages happen before the heavy pipeline starts, so the user sees the column profile and DAG nodes in the chat before committing to analysis.

### Stage 1 — Profile (pipeline internal)

`run_full_pipeline()` always re-runs the profiler at pipeline start to get a confirmed profile. If `/profile` was already called, this is a fast repeat. Sets `state.raw_profile`, `state.semantic_map`, `state.dataset_type`. Posts `PROFILE_COMPLETE` A2A message to `discovery_agent`.

### Stage 2 — Discovery (pipeline internal)

If `state.dag` was already set by the `/discover` endpoint, the pre-built DAG is reused (common path). Otherwise, the Discovery Agent is invoked to build the DAG from scratch. Policy filters are applied (`apply_policy_to_dag()`). Approved metrics filter is applied if provided. Final DAG is stored on `state.dag`. A `PipelineState` is created to track node execution. Posts `PLAN_READY` A2A message.

### Stage 3 — Execute DAG (`_execute_dag()`)

- Nodes are executed in topological order respecting `depends_on` edges.
- Up to **2 nodes run concurrently** (asyncio semaphore, `orchestrator.py` line 800).
- For each node:
  1. If a registered `library_function` exists, the orchestrator builds the Python call directly without an LLM call (`_build_library_call_code()`).
  2. Otherwise, the Coder Agent is invoked to write a `def analyze(csv_path: str) -> dict` function.
  3. Code is validated (`validate_code()`): syntax, safety (no `eval`/`exec`/`subprocess`/file deletion), dry run on 5 rows.
  4. Code is executed (`execute_analysis()`): the `analyze()` function runs, `chart_ready_data` is passed to chart generator.
  5. Quality is validated (`validate_output_quality()`): result has `status`, non-empty `top_finding`, non-empty `data`.
  6. If step fails, automatic column-role correction is attempted (`_attempt_column_role_correction()`) before the one retry.
  7. Per-node decision-maker insight is generated via direct Gemini API call (#10): one focused sentence injected into `insight_summary.decision_maker_takeaway`.
  8. Finding is appended to `_global_threads[session_id]` for injection into subsequent node prompts (#11).
  9. Behavioral analyses (funnel, friction, survival, etc.) automatically use `*_sessions.csv` if it exists (produced by `session_detection`).
- After all nodes, `check_failure_threshold()` alerts if >30% of nodes failed.
- SSE `node_complete` events are pushed after each success.

### Stage 4 — Synthesis

- `build_synthesis_prompt()` assembles: DAG graph, pre-extracted fact sheet (key metrics per node), full result data, chart PNG paths, user goal, and dataset profile.
- The Synthesis Agent is wrapped in a `LoopAgent` (max 3 outer iterations, #7).
- Inside each iteration: the agent calls `tool_aggregate_results()` then `tool_submit_synthesis()`.
- `tool_submit_synthesis()` applies a **multi-stage quality guard**: minimum character lengths for `overall_health` (150), each `ai_summary` (80), each `root_cause_hypothesis` (60), `conversational_report` (1500), required section headers (`# Key Findings`, `# Action Roadmap`, `# Confidence Assessment`), and minimum 2 `cross_metric_connections` each citing 2 distinct node IDs.
- If rejected, `reasoning_notes` from the failed attempt are stored in `_reasoning_store` and fed back into the retry so the LLM builds on prior work.
- A `LoopAgent` terminator checks `_qc_passed` flag; if set, it calls `tool_context.actions.escalate = True` to stop the loop.
- Synthesis is stored in `_synthesis_store[session_id]`, `state.synthesis`, and `output_folder/_synthesis_cache.json` (file-based backup).
- If `_synthesis_store` is still empty after the LoopAgent, up to 2 additional plain retries are attempted with 30s/60s delays.
- SSE `synthesis_complete` event is pushed.

### Stage 4.5 — Adversarial Critic (#8)

- 8-second delay to avoid rate-limiting after synthesis.
- Critic Agent calls `tool_get_synthesis_for_critique()` (reads `_synthesis_store` + rebuilds fact_sheet), then `tool_submit_critique()`.
- Critic result is injected into `_synthesis_store[session_id]["_critic_review"]` and `state.synthesis["_critic_review"]`.
- Non-fatal: if the critic crashes, pipeline continues.

### Stage 5 — Report Build

- `dag_builder_agent` calls `tool_build_report(session_id, output_folder)`.
- Collects chart HTML files from `state.results` + any orphan `.html` files in the output folder.
- Reads synthesis from `_synthesis_store` (primary), `state.synthesis` (secondary), `_synthesis_cache.json` (tertiary).
- Renders `report.html` with embedded chart iframes, synthesis sections, and critic review badge.
- Writes `synthesis.json` alongside the report.
- SSE `report_ready` event is pushed; frontend fetches and renders the report inline.
- Schema fingerprint is registered in `schema_registry.json` for future drift detection.

---

## 5. Agent Descriptions

### Profiler Agent (`agents/profiler.py`)

**Purpose:** Examines raw statistical facts about the dataset and produces a precise semantic classification.

**Tools:** `tool_profile_and_classify(csv_path, session_id)` — calls `csv_profiler.profile_csv()` and returns raw column statistics (types, cardinality, nulls, correlations, sample rows) back to the LLM.

**Output JSON:**
```json
{
  "status": "success",
  "raw_profile": { ... },
  "classification": {
    "dataset_type": "event_log",
    "confidence": 0.92,
    "reasoning": "...",
    "column_roles": {
      "entity_col": "user_uuid",
      "time_col": "created_at",
      "event_col": "action",
      "outcome_col": null,
      "funnel_col": null
    },
    "recommended_analyses": ["session_detection", "funnel_analysis", "dropout_analysis"]
  }
}
```

**Dataset types recognized:** `event_log`, `transactional`, `time_series`, `funnel`, `survey_or_cross_sectional`, `tabular_generic`.

**Key rule:** Column roles are inferred from data shape (cardinality, dtype, uniqueness ratio), never from column name keywords.

---

### Discovery Agent (`agents/discovery.py`)

**Purpose:** Reasons about the profiler output and designs a dependency-correct analysis DAG of 4–9 nodes.

**Tools:** `tool_submit_analysis_plan(session_id, dag_json_str)` — parses, alias-remaps, validates, and stores the DAG plan.

**Alias map (transparent LLM remapping):**
- `path_analysis` → `user_journey_analysis`
- `retention_analysis` → `cohort_analysis`
- `drop_off_analysis` → `dropout_analysis`
- `segment_analysis` → `user_segmentation`
- etc.

**Fallback:** If the LLM fails to submit a valid plan, `build_fallback_discovery()` in `main.py` builds a deterministic DAG from `recommended_analyses` in the profiler output — zero LLM calls.

**Feasibility rules:** If `entity_col` is null, all behavioral analyses are skipped. If `time_col` is null, trend/cohort/RFM analyses are skipped. If dataset has fewer than 50 rows, cohort/sequential analyses are skipped.

**Key constraint:** Max 10 nodes. `session_detection` must be the first node if any behavioral analysis is selected, and all behavioral nodes must declare it in `depends_on`.

---

### Coder Agent (`agents/coder.py`)

**Purpose:** Pure code-writer. Given an analysis spec, returns a single Python code block — no tools, no execution, no validation.

**Tools:** None.

**Output:** A `def analyze(csv_path: str) -> dict` function returning the standard result envelope: `{status, analysis_type, top_finding, data, chart_ready_data}`.

**Core rule:** If `library_function` is provided in the spec, the agent MUST call it directly. Writing custom pandas code to replicate a library function is explicitly forbidden in the system prompt.

**Column role mapping:** The agent maps `column_roles` dict (e.g. `{"entity_col": "user_uuid"}`) to function arguments, passing the actual column name string, not the role key name.

**Serialization rules:** All `numpy.int64`/`float64` must be converted; no `NaN`/`inf`; no raw NumPy arrays in the output dict.

**Error recovery:** If `validation_errors` are provided in the prompt (retry path), the agent reads them and fixes the specific issue rather than regenerating from scratch.

---

### Synthesis Agent (`agents/synthesis.py`)

**Purpose:** The final interpretation layer. Transforms all node results into a single, definitive, evidence-backed intelligence report.

**Tools:**
- `tool_aggregate_results(session_id)` — collects all results from `state.results`, groups by type and ID, builds `fact_sheet` (pre-extracted key metrics per node), returns `column_roles` and `dataset_type`.
- `tool_submit_synthesis(session_id, synthesis_json_str, reasoning_notes)` — validates quality, rejects with feedback if below minimum standards, otherwise stores to `_synthesis_store`.

**Output structure:**
```
executive_summary:
  overall_health, top_priorities, business_impact, resource_allocation, timeline
detailed_insights:
  insights[]: title, fix_priority, ai_summary, root_cause_hypothesis, ux_implications,
              possible_causes[], how_to_fix[], node_ids_referenced[]
cross_metric_connections:
  connections[]: finding_a, finding_b, synthesized_meaning, confidence, action_priority
personas (optional):
  personas[]: name, profile, pain_points[], opportunities[], priority_level
intervention_strategies (optional):
  strategies[]: title, severity, realtime_interventions[], proactive_outreach[]
conversational_report: (markdown narrative with # Key Findings, # Action Roadmap, # Confidence Assessment)
```

**Quality guard thresholds (enforced in `tool_submit_synthesis()`):**
- `executive_summary.overall_health` ≥ 150 chars
- Each `ai_summary` ≥ 80 chars
- Each `root_cause_hypothesis` ≥ 60 chars
- `conversational_report` ≥ 1500 chars
- Required headers: `# Key Findings`, `# Action Roadmap`, `# Confidence Assessment`
- ≥ 2 `cross_metric_connections`, each citing 2 distinct `[AX]` node IDs
- ≥ 3 total `[AX]` citations in the full output (soft warning, not block)

**Anti-hallucination:** The synthesis agent is explicitly forbidden from stating any number not in a tool result, claiming correlations not from a `correlation_matrix` node, or naming personas not derived from a `user_segmentation` node.

---

### Critic Agent (`agents/critic.py`)

**Purpose:** Adversarial peer reviewer. Checks the synthesis for 5 problem types: unsupported claims, contradictions, overconfidence, missed critical findings, vague actions.

**Tools:**
- `tool_get_synthesis_for_critique(session_id)` — retrieves synthesis + fact_sheet + insight text.
- `tool_submit_critique(session_id, approved, challenges, confidence_adjustment, overall_verdict)` — stores critique in `_critic_store`.

**Approval logic:** `approved=True` if 0–1 high-severity challenges; `approved=False` if 2+ high-severity. `confidence_adjustment` starts at 1.0, decrements 0.1 per high challenge, 0.05 per medium.

**Non-fatal:** Critic crashes do not stop the pipeline. Result is injected into synthesis as `_critic_review` key if available.

---

### DAG Builder Agent (`agents/dag_builder.py`)

**Purpose:** Assembles the final HTML report from charts and synthesis.

**Tools:** `tool_build_report(session_id, output_folder)` — reads charts from `state.results` and output folder, reads synthesis from a 3-source fallback chain (`_synthesis_store` → `state.synthesis` → `_synthesis_cache.json`), renders `_build_report_html()`, writes `report.html` and `synthesis.json`.

**Output:** Self-contained HTML with all Plotly charts embedded inline, synthesis sections (executive summary, insight cards, cross-metric connections, personas, critic review badge), and metadata.

---

### Chat Agent (`agents/chat_agent.py`)

**Purpose:** Conversational Q&A over completed analysis results. No tools, no pipeline triggers.

**Two modes:**
1. **Pre-pipeline** (state is `uploaded`/`profiled`/`discovered`): stores message as `state.user_instructions`, which are injected into the discovery prompt when analysis runs.
2. **Post-pipeline** (state is `complete`): full context (synthesis, all findings, column profile, column roles, recommendations) is assembled and injected into the prompt. Agent answers from this context only.

**Tools:** None. Answers from context injected in the prompt.

---

## 6. Analysis Library

All 26 registered analysis types in `LIBRARY_REGISTRY` (`tools/analysis_library.py`):

| Analysis Type | Function | Required Args (besides csv_path) | What It Computes | Required Column Roles |
|---|---|---|---|---|
| `distribution_analysis` | `run_distribution_analysis` | `col` | Histogram, box plot, skewness, kurtosis, IQR + Z-score outlier detection, normality test | Any numeric column |
| `categorical_analysis` | `run_categorical_analysis` | `col` | Frequency table, Pareto curve, entropy score, dominant categories, 80/20 split | Any categorical column |
| `correlation_matrix` | `run_correlation_matrix` | (none) | Pearson + Spearman correlation across all numeric columns, notable pairs | None |
| `anomaly_detection` | `run_anomaly_detection` | `col` | IQR, Z-score, and Isolation Forest consensus outlier detection | Any numeric column |
| `missing_data_analysis` | `run_missing_data_analysis` | (none) | Per-column null counts, overall null %, complete rows %, systematic pattern detection | None |
| `trend_analysis` | `run_trend_analysis` | `time_col`, `value_col` | Rolling averages, Mann-Kendall significance test, changepoint detection | `time_col`, any numeric |
| `time_series_decomposition` | `run_time_series_decomposition` | `time_col`, `value_col` | STL decomposition into trend, seasonality, residual noise | `time_col`, any numeric |
| `cohort_analysis` | `run_cohort_analysis` | `entity_col`, `time_col`, `value_col` | Groups by first-seen date, tracks activity in subsequent periods, retention by cohort | `entity_col`, `time_col`, numeric |
| `session_detection` | `run_session_detection` | `entity_col`, `time_col` | Infers session boundaries via time-gap heuristics; produces `session_id` column in `*_sessions.csv` | `entity_col`, `time_col` |
| `funnel_analysis` | `run_funnel_analysis` | `entity_col`, `event_col`, `time_col` | Step-by-step conversion rates, biggest drop-off step and %, overall conversion | Behavioral |
| `friction_detection` | `run_friction_detection` | `entity_col`, `event_col` | Events repeated abnormally often per session (friction score), critical/high event counts | Behavioral |
| `survival_analysis` | `run_survival_analysis` | `entity_col`, `event_col` | Kaplan-Meier survival curve, session half-life, critical drop-off step and rate | Behavioral |
| `user_segmentation` | `run_user_segmentation` | `entity_col`, `event_col`, `time_col` | DBSCAN behavioral clustering, segment count, segment characteristics, noise % | Behavioral |
| `sequential_pattern_mining` | `run_sequential_pattern_mining` | `entity_col`, `event_col` | PrefixSpan frequent ordered event sequences, top mini-journeys | Behavioral |
| `association_rules` | `run_association_rules` | `entity_col`, `event_col` | IF-THEN event co-occurrence rules with support, confidence, lift | Behavioral |
| `transition_analysis` | `run_transition_analysis` | `entity_col`, `event_col`, `time_col` | Markov transition matrix, exit probabilities, dead-end events, loops | Behavioral |
| `dropout_analysis` | `run_dropout_analysis` | `entity_col`, `event_col`, `time_col` | Last N events before session end, early exit %, top dropout sequences | Behavioral |
| `user_journey_analysis` | `run_user_journey_analysis` | `entity_col`, `event_col` | Per-entity path progression, common entry/exit events, avg/max steps | Behavioral |
| `event_taxonomy` | `run_event_taxonomy` | `event_col` | Auto-classifies events into 9 functional categories via keyword matching, category distribution | Any categorical (event) |
| `pareto_analysis` | `run_pareto_analysis` | `category_col`, `value_col` | 80/20 Pareto: which categories drive the top 20% of a value metric | Category + numeric |
| `rfm_analysis` | `run_rfm_analysis` | `entity_col`, `time_col`, `value_col` | RFM (Recency, Frequency, Monetary) segmentation into value tiers | `entity_col`, `time_col`, numeric |
| `contribution_analysis` | `run_contribution_analysis` | `group_col`, `value_col` | % contribution of each group to total value, variance per group | Category + numeric |
| `cross_tab_analysis` | `run_cross_tab_analysis` | `col_a`, `col_b` | Chi-squared test and Cramér's V between two categorical variables | Two categoricals |
| `intervention_triggers` | `run_intervention_triggers` | `entity_col`, `event_col`, `time_col` | Events that reliably precede process abandonment (>80% dropout rate), ranked by risk level | Behavioral |
| `session_classification` | `run_session_classification` | `entity_col`, `event_col`, `time_col` | Classifies entities into behavioral archetypes (depth, diversity, outcome signals) | Behavioral |
| `path_analysis` | `run_user_journey_analysis` | `entity_col`, `event_col` | Alias for `user_journey_analysis` (same function) | Behavioral |

**Alias entries also in registry:** `retention_analysis` (→ cohort_analysis), `time_to_event_analysis` (→ trend_analysis). These exist for LLM disambiguation but the Discovery Agent is instructed not to use them directly.

**Standard result envelope** (all functions return this shape):
```python
{
    "status": "success" | "error" | "insufficient_data",
    "analysis_type": str,
    "top_finding": str,  # one human-readable sentence with a specific number
    "data": dict,        # analysis-type-specific metrics
    "chart_ready_data": dict,  # Plotly-compatible chart spec with "type" key
    "severity": "info" | "warning" | "error" | "critical",
    "confidence": float 0.0–1.0,
}
```

---

## 7. Data Flow

### Session State (`main.py`, `SessionState` class)

Each session has one `SessionState` object in `sessions[session_id]` (in-memory Python dict). Key fields:

| Field | Type | Purpose |
|---|---|---|
| `csv_path` | str | Path to the normalized CSV |
| `raw_profile` | dict | Output of `csv_profiler.profile_csv()` |
| `semantic_map` | dict | LLM classification: column_roles, dataset_type, recommended_analyses |
| `dataset_type` | str | e.g. `event_log`, `transactional` |
| `dag` | list | List of DAG node dicts from Discovery Agent |
| `results` | dict | `{node_id: result_dict}` for all completed nodes |
| `failed_nodes` | set | Node IDs where execution returned `status=error` |
| `synthesis` | dict | Final synthesis JSON from Synthesis Agent |
| `artifacts` | list | `[{type, path, created}]` — includes report.html entry |
| `message_log` | list | A2A messages posted by agents during the pipeline |
| `user_instructions` | str | Instructions entered in chat before analysis starts |
| `gate_result` | dict | Pre-flight quality check result |

### Module-Level Stores (across agents)

| Store | Location | Purpose |
|---|---|---|
| `_plan_store` | `agents/discovery.py` | Discovery Agent submits DAG here; orchestrator reads with `.pop()` |
| `_profile_store` | `agents/profiler.py` | Profiler Agent result; read with `.pop()` |
| `_result_store` | `tools/code_executor.py` | `{session_id:node_id: result}` for all executed analyses |
| `_synthesis_store` | `agents/synthesis.py` | Stores validated synthesis JSON; read by critic, dag_builder, chat |
| `_reasoning_store` | `agents/synthesis.py` | LLM reasoning notes from failed synthesis attempts; injected into retries |
| `_critic_store` | `agents/critic.py` | Stores critic review result |
| `_report_store` | `agents/dag_builder.py` | Stores report path and metadata |
| `_pipeline_store` | `agents/orchestrator.py` | `{session_id: PipelineState}` for DAG execution tracking |
| `_global_threads` | `agents/orchestrator.py` | `{session_id: [{id, type, finding, dm_insight}]}` — accumulated findings (#11) |
| `_pipeline_event_hooks` | `agents/orchestrator.py` | `{session_id: callable}` — SSE push hooks registered by main.py (#12) |
| `_sse_events` | `main.py` | `{session_id: [{type, data}]}` — SSE event queue |

### A2A Message Flow

Messages are `A2AMessage` objects stored in `state.message_log`. They record intent-tagged communications between agent roles. Intents used:

| Intent | Sender → Recipient | When |
|---|---|---|
| `PROFILE_COMPLETE` | profiler_agent → discovery_agent | After profiling |
| `PLAN_READY` | discovery_agent → orchestrator | After discovery |
| `ANALYSIS_COMPLETE` | coder_agent → orchestrator | After each node success |
| `ANALYSIS_FAILED` | coder_agent → orchestrator | After node failure (implicit via state) |
| `SYNTHESIS_COMPLETE` | synthesis_agent → orchestrator | After synthesis stored |
| `REPORT_READY` | dag_builder_agent → orchestrator | After report written |
| `BUILD_REPORT` | orchestrator → frontend | Pipeline complete notification |

Messages are currently used for audit trail and post-pipeline introspection (e.g. by the `/messages` endpoint if implemented). The `is_dependency_resolved()` and `get_completed_analysis_ids()` functions in `a2a_messages.py` provide utilities for dependency checking against the message log, but actual DAG execution dependency resolution is done via `PipelineState.get_ready_to_run()` (which reads node status directly, not messages).

### SSE Event Types (Frontend-Visible)

| Event Type | When Fired | Frontend Action |
|---|---|---|
| `node_complete` | After each DAG node succeeds | Updates terminal card, renders new charts, updates metric cards |
| `synthesis_complete` | After synthesis stored | Advances stage 5 to active |
| `report_ready` | After `report.html` written | Advances stage 6 to active |
| `stream_end` | When pipeline reaches `complete` or `error` | Closes EventSource, calls `finishPipeline()` |

---

## 8. Key Parameters & Rules

### Model Configuration (`tools/model_config.py`)

All agents use the same model, defaulting to `gemini-3.1-pro-preview`. Override with `GEMINI_MODEL` environment variable. All agents share one model string — there is no per-agent model differentiation at present.

### Rate Limit Handling (`main.py`, `run_agent_pipeline()`)

- Max retries: **6 attempts** per agent invocation.
- On `429`/`rate`/`exhausted`/`quota`/`resource_exhausted` error, the retry delay is: `min(base_delay × (attempt+1), 120)` seconds (capped at 2 minutes).
- `base_delay`: extracted from the API error's `retryDelay` field if present; otherwise defaults to 20s.
- After 6 failed attempts, returns an error string rather than raising.

### Max Turns (`run_agent_pipeline()`)

- Default `max_turns=15` — stops the agent after 15 LLM round-trips.
- Critic specifically uses `max_turns=8`.
- Discovery uses `max_turns=12`.

### DAG Execution

- **Concurrency:** 2 nodes in parallel (asyncio Semaphore at line 800, `orchestrator.py`).
- **Retry:** Each node gets 1 automatic retry. On first failure, `_attempt_column_role_correction()` tries to fix column role mismatches before the retry.
- **Blocked nodes:** If a parent node fails, all dependent child nodes are marked `blocked` and skipped.
- **Max rounds:** `len(dag) + 2` rounds maximum to prevent infinite loops.

### Synthesis Quality Guard (thresholds in `tool_submit_synthesis()`)

| Check | Minimum | Action on Fail |
|---|---|---|
| `executive_summary.overall_health` length | 150 chars | Reject |
| Each `ai_summary` length | 80 chars | Reject |
| Each `root_cause_hypothesis` length | 60 chars | Reject |
| `conversational_report` length | 1500 chars | Reject |
| Required section headers | 3 headers | Reject |
| `cross_metric_connections` count | 2 entries | Reject |
| Each connection node citations | 2 distinct `[AX]` IDs | Reject |
| Total `[AX]` citation count | 3 | Warning only (emits monitor event) |

### Data Gate Thresholds (`tools/data_gate.py`)

| Check | Warning Threshold | Block Threshold |
|---|---|---|
| Null density (any column) | > 30% | > 90% |
| Duplicate rows | > 20% | N/A |
| Row count | < 10 rows | N/A (warning) |
| Column count | < 2 columns | N/A (warning) |

### Failure Rate Monitor (`tools/monitor.py`)

- `check_failure_threshold()`: fires `HIGH_FAILURE_RATE` error event if `failed_nodes / total_nodes > 0.30`.

### Synthesis Retry Logic

- LoopAgent: max **3 outer iterations**, each with inner quality rejection loop.
- After LoopAgent: up to **2 additional plain retries** with 30s and 60s waits if `_synthesis_store` is empty.
- Between synthesis and critic: **8-second sleep** to avoid rate limiting.

---

## 9. Frontend

### Overview (`static/app.js`)

The frontend is a single-page chat interface. There is no framework — all DOM manipulation is vanilla JS. The `$` helper aliases `document.querySelector`.

### Key Global State

| Variable | Purpose |
|---|---|
| `sessionId` | Current pipeline session ID |
| `outputFolder` | Folder name for the session's output files |
| `pendingFile` | File attached but not yet uploaded |
| `renderedCharts` | Set of analysis IDs already rendered (prevents duplicates) |
| `pollInterval` | Interval handle for polling fallback |
| `_activeSSE` | Active EventSource instance |
| `_terminalCard` | DOM reference to the live terminal card |
| `_terminalNodeStates` | `{node_id: status}` for terminal display |

### Upload & Pipeline Flow

1. `attachFile(file)` — stores file, shows attach preview.
2. `handleSend()` — if file attached, calls `uploadAndAnalyze(file, instructions)`.
3. `uploadAndAnalyze()`:
   - `POST /upload` → gets `session_id`.
   - If instructions, `POST /instructions/{session_id}`.
   - `POST /profile/{session_id}` → calls `addProfileMessage(data)`.
   - `POST /discover/{session_id}` → calls `addDiscoveryMessage(data)`.
4. After discover, a "Run Analysis" button appears. On click: `runAnalysis()`.
5. `runAnalysis()` calls `POST /analyze/{session_id}` (fire-and-forget), then calls `startSSEStream()`.

### SSE Stream (`startSSEStream()`)

- Opens `EventSource` on `/stream/{session_id}`.
- On `node_complete`: calls `updatePipelineStatusPanel()` (terminal + ring), `renderNewCharts()`, `updateMetricCards()`.
- On `synthesis_complete`: advances stage 5, starts "synthesize" rotating status messages.
- On `report_ready`: advances stage 6.
- On `stream_end`: calls `finishPipeline()`.
- On `onerror` (no events ever received): falls back to `_startPolling()`.
- Safety timeout: switches to polling if `stream_end` not received within `MAX_POLL_TIME` (10 minutes).

### Polling Fallback (`_startPolling()`)

- Polls `GET /status/{session_id}` every 2500ms.
- Reads `session_status` to advance stage indicators.
- Calls `renderNewCharts()` and `updatePipelineStatusPanel()` each tick.
- Times out at 10 minutes.

### Terminal Card

- Rendered inside an AI chat bubble.
- Shows a mock terminal with one line per DAG node: pending → running → complete/failed.
- `addTerminalCard(nodeCount)` creates the DOM element once.
- `updateTerminalCard(nodes)` updates individual node lines in place.

### Progress Ring

- SVG circular progress indicator.
- `updateProgressRing(completed, total)` sets stroke-dashoffset to reflect completion %.
- Shown only while pipeline is running.

### Key UI Functions

| Function | Purpose |
|---|---|
| `uploadAndAnalyze(file, instructions)` | Full pre-analysis flow: upload → profile → discover |
| `runAnalysis()` | Fires `/analyze`, starts SSE |
| `startSSEStream()` | Opens SSE connection, handles all pipeline events |
| `finishPipeline()` | Fetches synthesis + report HTML, renders them in chat |
| `renderNewCharts()` | Fetches `/api/session/{id}/charts`, renders any unseen charts |
| `updatePipelineStatusPanel()` | Calls `/api/session/{id}/status`, updates terminal card + ring + gate warnings |
| `addProfileMessage(data)` | Renders column profile as AI message |
| `addDiscoveryMessage(data)` | Renders DAG node list as AI message |
| `addAIMessage(text)` | Appends AI bubble to chat |
| `addUserMessage(text, isFile)` | Appends user bubble to chat |
| `sendChatMessage(text)` | Posts to `/chat/{session_id}`, renders response |
| `startProcessingStatus(phase)` | Starts rotating status messages for a pipeline phase |
| `setProgress(pct)` | Updates the top progress bar |
| `updateStage(n, status)` | Updates sidebar stage indicator |

### Rotating Status Messages

`STATUS_MESSAGES` object defines per-phase message pools (upload: 16 messages, profile: 23, discover: 20, analyze: 42, synthesize: 14, report: 18). Messages rotate every 2500ms with a 150ms fade transition. These are cosmetic only — they do not reflect actual pipeline sub-steps.

---

## 10. Recent Features

### #5 — Synthesis Rerun (`/rerun-synthesis`)

`POST /rerun-synthesis/{session_id}` re-runs synthesis (and critic + dag_builder) without re-running any analysis nodes. Clears `_synthesis_store`, `_reasoning_store`, and `state.synthesis` before starting. Implemented in `main.py` at line 764. Used when synthesis quality was poor. The shared `build_synthesis_prompt()` helper (`orchestrator.py` line 69) is extracted precisely so this endpoint can call it without duplicating code.

### #7 — LoopAgent for Synthesis Resilience

The synthesis stage is wrapped in a `google.adk.agents.LoopAgent` (`max_iterations=3`). Two sub-agents: the Synthesis Agent (writes the synthesis) and a lightweight terminator agent that calls `_check_synthesis_done()`. The terminator checks `_synthesis_store[session_id]["_qc_passed"]`; if True, it calls `tool_context.actions.escalate = True` to stop the loop. This provides outer-retry resilience on top of the inner quality-rejection loop inside `tool_submit_synthesis()`. Falls back to direct synthesis call if LoopAgent import fails.

### #8 — Adversarial Critic

Stage 4.5 in the pipeline. A separate `critic_agent` reviews the completed synthesis for five problem types. Result stored in `_critic_store` and injected into `_synthesis_store[session_id]["_critic_review"]`. The HTML report includes a critic review section. Non-fatal — pipeline continues if critic crashes. Implemented in `agents/critic.py`.

### #10 — Per-Node Decision-Maker Insight

Immediately after each DAG node result is stored, a direct `google.genai.Client().models.generate_content()` call (not through the ADK Runner) asks the LLM: "What is the single most important thing a decision-maker needs to act on?" The one-sentence answer is stored in `result["insight_summary"]["decision_maker_takeaway"]`. The Synthesis Agent is instructed to use these as starting points. Implemented in `orchestrator.py` around line 992.

### #11 — Global Reasoning Thread

`_global_threads[session_id]` accumulates completed node findings (id, type, top_finding, dm_insight) as the DAG executes. Each new node receives the last 5 non-parent entries as context in its coder prompt, enabling later analyses to reference earlier findings even without a formal DAG dependency edge. Synthesis receives this implicitly via the fact_sheet. Initialized in `_execute_dag()` at line 664.

### #12 — SSE Streaming

Before the pipeline starts, `main.py` registers a lambda `push_sse_event()` into `_pipeline_event_hooks[session_id]`. The orchestrator calls this hook after node completions, synthesis, and report completion. The `/stream/{session_id}` endpoint returns a `StreamingResponse` that polls `_sse_events[session_id]` every 0.8s and yields any new events. Frontend uses `EventSource` with polling fallback. Implemented across `main.py` (lines 726–761, 626–629) and `orchestrator.py` (lines 35–37, 501–506, 596–601).
