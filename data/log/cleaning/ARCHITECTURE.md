# ADK Analytics — System Architecture

> Multi-agent, domain-agnostic CSV analytics pipeline built on Google ADK.
> Upload any dataset. Get profiling, analysis, charts, and a full HTML report — automatically.

---

## Table of Contents

1. [What This System Does](#1-what-this-system-does)
2. [Tech Stack](#2-tech-stack)
3. [High-Level Architecture](#3-high-level-architecture)
4. [Design Philosophy](#4-design-philosophy)
5. [Agent Map](#5-agent-map)
6. [Tool Modules](#6-tool-modules)
7. [The 5-Stage Pipeline](#7-the-5-stage-pipeline)
8. [DAG Execution Model](#8-dag-execution-model)
9. [A2A Messaging Protocol](#9-a2a-messaging-protocol)
10. [Core Data Structures](#10-core-data-structures)
11. [Frontend Architecture](#11-frontend-architecture)
12. [API Endpoints](#12-api-endpoints)
13. [Analysis Library](#13-analysis-library)
14. [Model Configuration](#14-model-configuration)
15. [File Map](#15-file-map)
16. [Key Architectural Decisions](#16-key-architectural-decisions)
17. [Deterministic Fallback System](#17-deterministic-fallback-system)
18. [Bug Fixes and Lessons Learned](#18-bug-fixes-and-lessons-learned)

---

## 1. What This System Does

This is a **domain-agnostic automated analytics system**. You upload any CSV (or Excel, JSON, Parquet) file and the system automatically:

1. **Profiles** the data — detects column types, roles (entity, timestamp, event, outcome), and dataset classification
2. **Discovers** which analyses are valuable — an LLM reasons about the data profile and builds a custom analysis plan
3. **Executes** each analysis — using pre-built library functions when available (zero LLM), or generating custom code via LLM when not
4. **Synthesizes** findings — generates executive summary, user personas, intervention strategies, cross-metric connections
5. **Assembles** a standalone HTML report with embedded Plotly charts

The system handles everything from simple tabular data to complex event logs with user sessions, funnels, and behavioral patterns.

---

## 2. Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Agent Framework** | Google ADK (Agent Development Kit) | Agent definitions, tool binding, LLM routing |
| **LLM Provider** | OpenAI GPT-4o | Reasoning, code generation, analysis planning |
| **LLM Routing** | LiteLLM (via ADK) | Routes `openai/` model strings to OpenAI API |
| **Backend** | FastAPI + Uvicorn | REST API server, file upload, session management |
| **Data Processing** | Pandas, NumPy, SciPy, Scikit-learn | All analysis computations |
| **Charts** | Plotly | Interactive HTML charts (22 chart types) |
| **Frontend** | Vanilla JS + CSS | Single-page app with polling, progressive rendering |
| **Messaging** | Custom A2A protocol (in-memory) | Agent-to-agent coordination |

---

## 3. High-Level Architecture

```
                        +------------------+
                        |    Frontend      |
                        |  (app.js + UI)   |
                        +--------+---------+
                                 |
                            REST API
                                 |
                        +--------+---------+
                        |    main.py       |
                        |  (FastAPI)       |
                        +--------+---------+
                                 |
              +------------------+------------------+
              |                                     |
     +--------v---------+                  +--------v---------+
     |   SessionState   |                  |  run_agent_pipeline|
     | (in-memory store) |                  | (ADK Runner)     |
     +------------------+                  +--------+---------+
                                                    |
                                    +---------------+---------------+
                                    |                               |
                           +--------v---------+           +--------v---------+
                           |   ORCHESTRATOR   |           |   A2A Messages   |
                           |  (coordinator)   |           | (message bus)    |
                           +--------+---------+           +------------------+
                                    |
                 +------------------+------------------+
                 |           |            |            |
        +--------v--+ +-----v-----+ +---v------+ +---v--------+
        | PROFILER  | | DISCOVERY | | CODER    | | SYNTHESIS  |
        | (sensor)  | | (brain)   | | (writer) | | (narrator) |
        +-----------+ +-----------+ +----------+ +------+-----+
                                                        |
                                                 +------v-----+
                                                 | DAG BUILDER|
                                                 | (formatter)|
                                                 +------------+

        +-------------------------------------------------------+
        |                    TOOLS LAYER                         |
        |  analysis_library | code_executor | csv_profiler       |
        |  model_config     | ingestion_normalizer               |
        +-------------------------------------------------------+
```

**Data flows down.** Agents call tools. Tools do the actual work (pure Python). LLM reasoning is only used where human-like judgment is needed.

---

## 4. Design Philosophy

### 4.1 Agent-Tool Separation

Every agent follows one rule: **agents reason, tools execute.**

- **Agents** are thin wrappers around an LLM with a focused instruction and a set of tools. They decide *what* to do.
- **Tools** are pure Python functions with zero LLM dependencies. They do the actual computation.

This means:
- You can fix a bug in chart generation without touching any agent
- You can swap the LLM provider without changing any tool
- You can test tools in isolation (no LLM needed)

### 4.2 Library-First Execution

The system has **20 pre-built analysis functions** in `tools/analysis_library.py`. When the orchestrator executes a DAG node:

1. Check if a library function exists for this analysis type
2. **If yes** → build the function call as a Python string, validate, execute. **Zero LLM calls.**
3. **If no** → call the coder agent (LLM) to write custom code

~95% of analyses use library functions. This makes the pipeline fast, deterministic, and cheap.

### 4.3 Each Agent Name = What It Does

| Agent | What it does | What it does NOT do |
|---|---|---|
| **Profiler** | Reads CSV, reports facts | Never suggests analyses |
| **Discovery** | Decides which analyses to run | Never reads CSV |
| **Coder** | Writes Python code | Never executes, validates, or stores results |
| **Orchestrator** | Coordinates everything, executes code | Never writes analysis code |
| **Synthesis** | Interprets results into narrative | Never reads CSV or runs analyses |
| **DAG Builder** | Assembles HTML report | Never interprets or analyzes |

### 4.4 Modularity

Each agent file is self-contained and small. If an agent misbehaves, you fix just that file. The `tools/` layer is shared infrastructure — pure Python, fully testable, no LLM coupling.

---

## 5. Agent Map

### 5.1 Orchestrator Agent (`agents/orchestrator.py`)

**Role:** Central coordinator. Manages the 5-stage pipeline and per-node DAG execution.

| Property | Value |
|---|---|
| Tools | 7 (pipeline management + sub-agent coordination) |
| Sub-agents called | Profiler, Discovery, Coder, Synthesis, DAG Builder |
| Model | Configurable via `get_model("orchestrator")` |

**Tools:**
- `tool_update_pipeline_status` — update status + A2A message
- `tool_run_profiler` — Stage 1: call Profiler (idempotent)
- `tool_run_discovery` — Stage 2: call Discovery (idempotent)
- `tool_execute_dag` — Stage 3: execute all DAG nodes
- `tool_run_synthesis` — Stage 4: call Synthesis
- `tool_build_report` — Stage 5: call DAG Builder
- `tool_get_pipeline_progress` — check progress between stages

**Key functions:**
- `run_full_pipeline()` — main async entry point, builds dynamic prompt listing only remaining stages
- `_fallback_pipeline()` — direct tool calls if orchestrator LLM fails
- `_execute_dag()` — async DAG engine with dependency resolution and parallel execution
- `_execute_single_node()` — per-node: precomputed check → library call → coder LLM → validate → execute → quality check → retry → submit
- `_build_library_call_code()` — constructs library function call as Python string (no LLM)
- `_get_code_from_coder()` — calls coder agent for custom code (LLM)
- `_extract_code_from_response()` — regex extracts Python code from LLM response
- `execute_single_analysis()` — wrapper for ad-hoc analysis outside DAG context

### 5.2 Profiler Agent (`agents/profiler.py`)

**Role:** Pure sensor. Reads CSV, reports facts. Makes zero decisions about what to analyze.

| Property | Value |
|---|---|
| Tools | 1 (`tool_profile_and_classify`) |
| Model | Configurable via `get_model("profiler")` |
| Output | raw_profile + semantic_map + classification |

**What it produces:**
- Row/column counts, column types, sample rows
- Per-column stats (mean, median, std, unique count, etc.)
- Correlations (pairs with |r| > 0.4)
- Semantic map: each column tagged as entity_identifier, timestamp, event_or_state, outcome_metric, etc.
- Dataset classification: event_log, transactional, time_series, funnel, or tabular_generic
- Column roles: entity_col, time_col, event_col, outcome_col, funnel_col
- Recommended analyses based on dataset type

### 5.3 Discovery Agent (`agents/discovery.py`)

**Role:** The brain. Reasons about which analyses are valuable for this specific dataset and builds a custom DAG.

| Property | Value |
|---|---|
| Tools | 4 |
| Model | Configurable via `get_model("discovery")` |
| Input | Profiler output + user instructions |
| Output | Analysis DAG (list of MetricSpec nodes) |

**Tools:**
- `tool_get_library_catalog` — returns all 20 available analysis functions with metadata
- `tool_build_analysis_dag` — wires selected analyses into DAG with correct dependencies
- `tool_validate_custom_request` — checks if a custom user request is feasible
- `tool_submit_analysis_plan` — stores final plan for orchestrator to execute

**Dependency rules enforced:**
- `session_detection` MUST run first for any behavioral analysis
- `association_rules` depends on `funnel_analysis`
- All other analyses are independent

### 5.4 Coder Agent (`agents/coder.py`)

**Role:** Pure code writer. Receives an analysis spec, returns Python code. Nothing else.

| Property | Value |
|---|---|
| Tools | 0 (zero — no tools at all) |
| Model | Configurable via `get_model("coder")` |
| Input | analysis_type, column_roles, description, library_function hint, validation_errors |
| Output | Python code block with `def analyze(csv_path) -> dict` |

The coder is only called when no library function exists for an analysis type, or when fixing validation/quality errors. For ~95% of nodes, the orchestrator builds the code string directly from library function metadata — no LLM needed.

### 5.5 Synthesis Agent (`agents/synthesis.py`)

**Role:** Interpretation layer. Transforms raw analysis results into human-readable narrative, personas, and strategies.

| Property | Value |
|---|---|
| Tools | 6 |
| Model | Configurable via `get_model("synthesis")` |
| Input | All completed AnalysisResult objects |
| Output | Executive summary, personas, strategies, cross-metric connections |

**Tools:**
- `tool_aggregate_results` — collects all results, groups by type
- `tool_generate_executive_summary` — 5-7 bullets with real numbers, severity tags
- `tool_generate_segment_personas` — named personas from clustering (Power Users, Struggling Users, etc.)
- `tool_generate_intervention_strategies` — ranked strategies with root cause, action, impact, complexity
- `tool_generate_cross_metric_connections` — finds reinforcing findings across analyses
- `tool_submit_synthesis` — stores final synthesis, posts A2A message

### 5.6 DAG Builder Agent (`agents/dag_builder.py`)

**Role:** Pure formatter. Takes charts and synthesis narrative, assembles the final standalone HTML report.

| Property | Value |
|---|---|
| Tools | 3 |
| Model | Configurable via `get_model("dag_builder")` |
| Input | Chart HTML files + synthesis object |
| Output | Standalone `report.html` |

**Tools:**
- `tool_collect_artifacts` — gathers all chart files and result summaries
- `tool_assemble_report` — builds full HTML with embedded charts (iframes with srcdoc)
- `tool_list_artifacts` — lists all generated files

The report is fully standalone — all charts embedded inline, no external dependencies. Can be opened offline.

---

## 6. Tool Modules

### 6.1 `tools/analysis_library.py`

**Purpose:** 20 pre-built, tested analysis algorithms. Every function returns a standardized result envelope.

**Key export:** `LIBRARY_REGISTRY` — dict mapping analysis_type to function metadata (name, required args, column role, description).

**Analysis functions by category:**

| Category | Functions |
|---|---|
| **Distribution** | `run_distribution_analysis`, `run_categorical_analysis`, `run_correlation_matrix` |
| **Anomaly/Quality** | `run_anomaly_detection`, `run_missing_data_analysis` |
| **Time Series** | `run_trend_analysis`, `run_time_series_decomposition` |
| **Cohort/Business** | `run_cohort_analysis`, `run_rfm_analysis`, `run_pareto_analysis` |
| **Behavioral** | `run_session_detection`, `run_funnel_analysis`, `run_friction_detection`, `run_survival_analysis`, `run_user_segmentation`, `run_sequential_pattern_mining`, `run_association_rules` |
| **Advanced** | `run_transition_analysis`, `run_dropout_analysis`, `run_event_taxonomy` |

**Used by:** Orchestrator (library lookup), Discovery (catalog display), Code Executor (function imports at runtime)

### 6.2 `tools/code_executor.py`

**Purpose:** Pure Python utilities for code validation, execution, chart generation, and result management. Extracted from the old coder agent to keep agents thin.

**Key functions:**

| Function | What it does |
|---|---|
| `lookup_library_function(type)` | Dict lookup into LIBRARY_REGISTRY |
| `check_precomputed_result(sid, type)` | Check session cache for existing result |
| `validate_code(code, csv_path)` | AST parse + safety check + dry-run on 5 rows |
| `execute_analysis(code, csv_path, ...)` | exec() in sandbox + chart generation |
| `validate_output_quality(result, type)` | Pattern checks for meaningful content |
| `submit_result(sid, aid, type, result)` | Store result + post A2A message |
| `generate_chart(data, id, type, folder)` | Plotly HTML chart generation (22 types) |
| `get_analysis_result(sid, aid)` | Read from module-level result cache |
| `store_analysis_result(sid, aid, result)` | Write to module-level result cache |

**Used by:** Orchestrator (all functions), main.py (result retrieval)

### 6.3 `tools/csv_profiler.py`

**Purpose:** Reads a CSV and produces raw facts. No decisions, no suggestions — just facts.

**Key functions:**
- `profile_csv(csv_path)` — comprehensive profiling (column stats, correlations, sample rows)
- `infer_column_semantics(profile)` — maps each column to semantic tag with confidence
- `classify_dataset(profile, semantic_map)` — determines dataset type and column roles

**Used by:** Profiler Agent (via `tool_profile_and_classify`)

### 6.4 `tools/ingestion_normalizer.py`

**Purpose:** Converts any supported file format into a clean single-table CSV.

**Supported formats:** CSV, XLSX, XLS, JSON, JSONL, Parquet

**Key function:** `normalize_file(file_path)` — handles encoding detection, multi-sheet Excel, nested JSON flattening, header detection, deduplication.

**Used by:** main.py (at upload time, before profiling)

### 6.5 `tools/model_config.py`

**Purpose:** Central model configuration. Provider switching via environment variable.

**Key function:** `get_model(agent_name)` — returns the model string for a given agent based on `MODEL_PROVIDER` env var.

**Used by:** All 6 agent files

---

## 7. The 5-Stage Pipeline

```
 Upload          Stage 1         Stage 2           Stage 3              Stage 4          Stage 5
+------+    +----------+    +------------+    +---------------+    +------------+    +-----------+
| File | -> | PROFILER | -> | DISCOVERY  | -> | EXECUTE DAG   | -> | SYNTHESIS  | -> | REPORT    |
| .csv |    | (sensor) |    | (brain)    |    | (per-node)    |    | (narrator) |    | (format)  |
+------+    +----------+    +------------+    +---------------+    +------------+    +-----------+
                |                |                   |                   |                |
           raw_profile      DAG with           Results for          Executive        report.html
           semantic_map     MetricSpec         each analysis        Summary          (standalone)
           column_roles     nodes              + charts             Personas
           dataset_type                                             Strategies
```

### Stage 1: Profile (`/profile/{session_id}`)

1. Profiler agent calls `tool_profile_and_classify(csv_path, session_id)`
2. Tool runs `profile_csv()` → `infer_column_semantics()` → `classify_dataset()`
3. Returns: raw profile, semantic map, column roles, dataset type
4. Frontend renders summary cards (row count, columns, types, date span)

### Stage 2: Discover (`/discover/{session_id}`)

1. Discovery agent receives profiler output + user instructions
2. Calls `tool_get_library_catalog()` to see available analyses
3. **Reasons** about which analyses are valuable (this is where LLM judgment matters)
4. Calls `tool_build_analysis_dag()` with selected analyses
5. Calls `tool_submit_analysis_plan()` to store the DAG
6. Frontend renders metric roadmap with severity badges

### Stage 3: Execute DAG (`/analyze/{session_id}`)

1. Orchestrator calls `tool_execute_dag()` which triggers `_execute_dag()`
2. DAG engine resolves dependencies, executes nodes in parallel where possible
3. For each node, `_execute_single_node()` runs the 8-step flow (see [DAG Execution Model](#8-dag-execution-model))
4. Frontend polls `/status` every 2s, progressively renders charts as they complete

### Stage 4: Synthesize

1. Synthesis agent collects all completed results
2. Generates: executive summary, personas, intervention strategies, cross-metric connections
3. Stores synthesis in session state

### Stage 5: Build Report

1. DAG Builder collects all chart HTML files + synthesis narrative
2. Assembles standalone `report.html` with all charts embedded as iframes
3. Frontend shows "View Report" and "Download Report" buttons

---

## 8. DAG Execution Model

### 8.1 Dependency Resolution

The DAG is a list of `MetricSpec` nodes, each with a `depends_on` list of node IDs. The execution engine in `_execute_dag()`:

1. Builds a dependency graph
2. Identifies nodes with all dependencies satisfied (or no dependencies)
3. Executes ready nodes in parallel using `ThreadPoolExecutor`
4. As nodes complete, checks if new nodes become ready
5. Repeats until all nodes are complete or failed

### 8.2 Per-Node Execution (`_execute_single_node`)

```
Step 1: check_precomputed_result()
  |-- exists? -> submit_result() -> DONE (zero cost)
  |
Step 2: lookup_library_function()
  |-- exists? -> _build_library_call_code() -> code (NO LLM)
  |-- not found? -> _get_code_from_coder() -> code (LLM call)
  |
Step 3: validate_code() -- AST parse, safety check, dry-run
  |-- fails? -> _get_code_from_coder(validation_errors=...) -> retry
  |
Step 4: execute_analysis() -- exec() in sandbox + chart gen
  |-- fails? -> retry with coder
  |
Step 5: validate_output_quality() -- pattern checks
  |-- fails? -> retry with coder (max 3 attempts)
  |
Step 6: submit_result() -- store + A2A message -> DONE
```

### 8.3 Library-First Optimization

This is the key performance insight:

- 20 of ~20 analysis types have library functions in `LIBRARY_REGISTRY`
- For these, the orchestrator builds the code string directly: `from tools.analysis_library import run_X; def analyze(csv_path): return run_X(...)`
- No LLM call needed. Just string construction, validation, execution.
- The coder LLM is only invoked for truly custom/novel analyses or when fixing errors.

**Result:** A 9-node DAG with all library analyses completes with **zero coder LLM calls**. Only the orchestrator LLM (for stage coordination) and discovery LLM (for analysis selection) are used.

### 8.4 Retry Logic

- Validation failures: re-call coder with error details, max 3 attempts
- Execution failures: re-call coder with error details, max 3 attempts
- Quality failures: re-call coder with quality issues, max 3 attempts
- After max retries: submit whatever result exists (even partial)
- Dependency failures: dependents marked as blocked, not retried

### 8.5 Behavioral Analysis Chain

For event log datasets, a special dependency chain exists:

```
session_detection (CRITICAL, runs first)
    |-- produces: enriched _sessions.csv with session_id column
    |
    +-> funnel_analysis
    +-> friction_detection
    +-> survival_analysis
    +-> user_segmentation
    +-> sequential_pattern_mining
    +-> transition_analysis
    +-> dropout_analysis
    +-> association_rules (also depends on funnel_analysis)
```

All behavioral analyses read from `_sessions.csv` instead of the original CSV.

---

## 9. A2A Messaging Protocol

### 9.1 Overview

Agents communicate via an in-memory message bus stored in `SessionState.message_log`. Each message has a sender, recipient, intent, and payload.

### 9.2 Intent Constants

```
Profiling:    PROFILE_REQUEST, PROFILE_COMPLETE
Discovery:    DISCOVER_METRICS, PLAN_READY, PLAN_APPROVED
Analysis:     RUN_ANALYSIS, ANALYSIS_COMPLETE, ANALYSIS_FAILED,
              DEPENDENCY_BLOCKED, DEPENDENCY_RESOLVED
Synthesis:    SYNTHESIZE, SYNTHESIS_COMPLETE
Report:       BUILD_REPORT, REPORT_READY
System:       STATUS_UPDATE, ERROR, TASK_COMPLETE,
              CLARIFICATION_NEEDED, CLARIFICATION_PROVIDED
```

### 9.3 Message Structure

```python
A2AMessage(
    sender="profiler_agent",
    recipient="discovery_agent",
    intent=Intent.PROFILE_COMPLETE,
    payload={"dataset_type": "event_log", "ready": True},
    session_id="abc123",
    message_id="auto-generated",
    timestamp="auto-generated",
)
```

### 9.4 Message Flow

```
Profiler  --PROFILE_COMPLETE--> Discovery
Discovery --PLAN_READY--------> Orchestrator
Coder     --ANALYSIS_COMPLETE-> Orchestrator (per node)
Synthesis --SYNTHESIS_COMPLETE-> Orchestrator
DAGBuilder--REPORT_READY------> Frontend (via status polling)
Orchestrator--STATUS_UPDATE----> Frontend (at each stage)
```

---

## 10. Core Data Structures

### 10.1 MetricSpec (DAG Node)

Defined in `a2a_messages.py`. Represents one analysis to execute.

```python
MetricSpec(
    id="A1",
    name="Session Detection",
    description="Identify user sessions from event stream",
    analysis_type="session_detection",
    library_function="run_session_detection",
    required_columns=["user_id", "timestamp", "action"],
    column_roles={"entity_col": "user_id", "time_col": "timestamp", "event_col": "action"},
    depends_on=[],
    enables=["A2", "A3"],
    priority="critical",
    feasibility="HIGH",
)
```

### 10.2 AnalysisResult (Output Envelope)

Standardized result from every analysis. Defined in `a2a_messages.py`.

```python
AnalysisResult(
    analysis_id="A1",
    analysis_type="session_detection",
    status="success",
    data={"total_sessions": 1234, "avg_events_per_session": 8.3},
    top_finding="Detected 1,234 sessions across 456 users. Average 8.3 events per session.",
    severity="info",
    confidence=0.92,
    chart_ready_data={"type": "session_length_histogram", "event_counts": [...]},
    chart_file_path="output/my_data/A1_session_detection.html",
)
```

### 10.3 PipelineState (Execution Tracker)

Tracks DAG execution progress. Defined in `a2a_messages.py`.

```python
PipelineState(
    session_id="abc123",
    total_nodes=9,
    nodes={"A1": "complete", "A2": "running", "A3": "pending"},
    completed=["A1"], running=["A2"], blocked=[], failed=[], pending=["A3",...],
)
```

**Methods:** `mark_running()`, `mark_complete()`, `mark_failed()`, `mark_blocked()`, `get_ready_to_run()`, `is_complete()`, `progress_pct()`

### 10.4 SessionState (Server-Side Session)

Defined in `main.py`. Tracks everything for one analysis session.

```python
SessionState(
    session_id="abc123",
    csv_path="/path/to/file.csv",
    csv_filename="my_data.csv",
    output_folder="my_data",
    status="analyzing",          # uploaded|profiling|profiled|discovering|discovered|analyzing|...
    raw_profile={...},           # from profiler
    semantic_map={...},          # column roles + types
    dataset_type="event_log",
    dag=[...],                   # list of MetricSpec dicts
    results={"A1": {...}},       # keyed by analysis_id
    synthesis={...},             # from synthesis agent
    artifacts=[...],             # report paths
    message_log=[...],           # A2A messages
    user_instructions="",        # from chat (pre-pipeline)
)
```

---

## 11. Frontend Architecture

### 11.1 Layout

```
+--sidebar (260px)--+--main content (flex-grow)----+
|                   |                               |
| Logo              |  VOL.01 Upload                |
|                   |  - Drop zone                  |
| Stage 1: Upload   |  - File info card             |
| Stage 2: Profile  |                               |
| Stage 3: Discover |  VOL.02 Summary Cards         |
| Stage 4: Analyze  |  - Row count, columns, types  |
| Stage 5: Synthesize|                              |
| Stage 6: Report   |  VOL.03 Metric Roadmap        |
|                   |  - Analysis cards with badges  |
| Session badge     |  - Execute button              |
|                   |                               |
+-------------------+  VOL.04 Results               |
                    |  - Executive summary           |
                    |  - Chart grid (progressive)    |
                    |  - Report buttons              |
                    |                               |
                    +--chat widget (bottom-right)----+
```

### 11.2 Polling Mechanism

After clicking "Execute Analysis Pipeline":

1. `POST /analyze/{session_id}` fires pipeline in background
2. Frontend polls `GET /status/{session_id}` every 2 seconds
3. Each poll: check for new results, render charts progressively, update sidebar stages
4. Stops when `session_status === 'complete'` or after 10 minutes

### 11.3 Progressive Chart Rendering

- `renderedCharts` Set tracks which analysis IDs are already displayed
- Each poll fetches `/results/{session_id}`, compares against rendered set
- New charts animate in with fade+slide-up transition
- Metric roadmap cards update with checkmark badges

### 11.4 Two-Mode Chat

- **Pre-pipeline** (status: uploaded/profiled/discovered): Messages stored as `user_instructions`, passed to discovery/analysis agents
- **Post-pipeline** (status: complete): Messages enriched with session results/synthesis for context-aware Q&A

### 11.5 Stage Indicators

Sidebar stages progress through: pending → active (with pulse animation) → complete (green dot)

The `minComplete = 3` guard ensures stages 1-3 (upload, profile, discover) never go backwards once the Execute button is clicked, even during polling.

---

## 12. API Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Serve main UI (index.html) |
| `POST` | `/upload` | Upload file, normalize to CSV, create session |
| `POST` | `/profile/{sid}` | Run profiler agent, return profile + classification |
| `POST` | `/discover/{sid}` | Run discovery agent, return analysis DAG |
| `POST` | `/validate-metric/{sid}` | Check if custom metric is feasible |
| `POST` | `/analyze/{sid}` | Launch full pipeline in background |
| `POST` | `/add-metric/{sid}` | Execute single ad-hoc analysis |
| `POST` | `/chat/{sid}` | Two-mode chat (pre/post pipeline) |
| `GET` | `/status/{sid}` | Poll pipeline progress |
| `GET` | `/results/{sid}` | Get all completed analysis results |
| `GET` | `/chart/{sid}/{aid}` | Serve chart HTML file |
| `GET` | `/synthesis/{sid}` | Get synthesis results |
| `GET` | `/report/{sid}` | Serve final HTML report |
| `GET` | `/output/{folder}/{file}` | Serve any generated artifact |
| `GET` | `/sessions` | List all active sessions |

---

## 13. Analysis Library

### 13.1 Available Analyses (20 functions)

| # | Analysis Type | Function | Input | Key Output |
|---|---|---|---|---|
| 1 | `distribution_analysis` | `run_distribution_analysis` | csv, col | Histogram, box plot, normality test, outliers |
| 2 | `categorical_analysis` | `run_categorical_analysis` | csv, col | Frequency table, Pareto, entropy |
| 3 | `correlation_matrix` | `run_correlation_matrix` | csv | Pearson correlations, notable pairs |
| 4 | `anomaly_detection` | `run_anomaly_detection` | csv, col | IQR + Z-score + Isolation Forest consensus |
| 5 | `missing_data_analysis` | `run_missing_data_analysis` | csv | Missingness patterns per column |
| 6 | `trend_analysis` | `run_trend_analysis` | csv, time_col, value_col | Rolling averages, Mann-Kendall test |
| 7 | `time_series_decomposition` | `run_time_series_decomposition` | csv, time_col, value_col | STL: trend + seasonal + residual |
| 8 | `cohort_analysis` | `run_cohort_analysis` | csv, entity, time, value | Retention by first-appearance cohort |
| 9 | `rfm_analysis` | `run_rfm_analysis` | csv, entity, time, value | Recency/Frequency/Monetary segments |
| 10 | `pareto_analysis` | `run_pareto_analysis` | csv, entity, value | 80/20 rule analysis |
| 11 | `session_detection` | `run_session_detection` | csv, entity, time, event | Creates enriched _sessions.csv |
| 12 | `funnel_analysis` | `run_funnel_analysis` | csv, entity, event, time | Conversion rates per step |
| 13 | `friction_detection` | `run_friction_detection` | csv, entity, event | High-repetition friction events |
| 14 | `survival_analysis` | `run_survival_analysis` | csv, entity, event | Kaplan-Meier survival curve |
| 15 | `user_segmentation` | `run_user_segmentation` | csv, entity, event, time | DBSCAN/KMeans behavioral clusters |
| 16 | `sequential_pattern_mining` | `run_sequential_pattern_mining` | csv, entity, event | PrefixSpan frequent sequences |
| 17 | `association_rules` | `run_association_rules` | csv, entity, event | IF-THEN rules with support/confidence/lift |
| 18 | `transition_analysis` | `run_transition_analysis` | csv, entity, event, time | Markov transition matrix |
| 19 | `dropout_analysis` | `run_dropout_analysis` | csv, entity, event, time | Last events before session end |
| 20 | `event_taxonomy` | `run_event_taxonomy` | csv, event | Auto-classify into 9 functional categories |

### 13.2 Chart Types (22)

`histogram_box`, `frequency_bar`, `correlation_heatmap`, `anomaly_scatter`, `missing_bar`, `trend_line`, `funnel_bar`, `friction_heatmap`, `survival_curve`, `segment_donut`, `pareto_bar`, `pareto_curve`, `rfm_donut`, `rfm_scatter`, `sequence_bar`, `cohort_heatmap`, `transition_heatmap`, `dropout_bar`, `taxonomy_donut`, `session_length_histogram`, `rules_card`, `decomposition`

### 13.3 Result Envelope Format

Every analysis function returns:

```python
{
    "analysis_type": "session_detection",
    "status": "success",                      # success | error | insufficient_data
    "data": {...},                             # computed metrics
    "top_finding": "Real insight with numbers",
    "severity": "info",                        # critical | high | medium | low | info
    "confidence": 0.92,
    "chart_ready_data": {"type": "...", ...},  # Plotly-compatible data
    "enables": [...],                          # downstream analyses
}
```

---

## 14. Model Configuration

### 14.1 Model Configuration

All agents use OpenAI GPT-4o via `tools/model_config.py`. Set `OPENAI_API_KEY` in `.env`.

Override the default model by setting `OPENAI_MODEL` in `.env` (default: `openai/gpt-4o`).

### 14.2 Model Mapping

| Agent | Model |
|---|---|
| orchestrator | `openai/gpt-4o` |
| profiler | `openai/gpt-4o` |
| discovery | `openai/gpt-4o` |
| coder | `openai/gpt-4o` |
| synthesis | `openai/gpt-4o` |
| dag_builder | `openai/gpt-4o` |

---

## 15. File Map

```
ADK/
+-- main.py                         FastAPI server, 14 endpoints, SessionState
+-- a2a_messages.py                 A2AMessage, MetricSpec, AnalysisResult, PipelineState
+-- .env                            API keys
+-- ARCHITECTURE.md                 This document
|
+-- agents/
|   +-- orchestrator.py             Pipeline coordinator, DAG execution, 7 tools
|   +-- coder.py                    Pure code writer, 0 tools
|   +-- profiler.py                 CSV profiling, 1 tool
|   +-- discovery.py                Analysis planning, 4 tools
|   +-- synthesis.py                Result interpretation, 6 tools
|   +-- dag_builder.py              HTML report assembly, 3 tools
|
+-- tools/
|   +-- __init__.py                 Package exports
|   +-- model_config.py             Provider switching, get_model()
|   +-- analysis_library.py         20 analysis functions + LIBRARY_REGISTRY
|   +-- code_executor.py            Validation, execution, chart gen, result store
|   +-- csv_profiler.py             Raw CSV profiling, column semantics, classification
|   +-- ingestion_normalizer.py     Multi-format file normalization
|   +-- chart_generator.py          (legacy — unused, kept for reference)
|   +-- file_manager.py             (legacy — unused, kept for reference)
|
+-- static/
|   +-- index.html                  UI layout: sidebar, 4 sections, chat, modal
|   +-- app.js                      Frontend logic: upload, polling, rendering, chat
|   +-- style.css                   Design system: colors, typography, animations
|
+-- output/                         Generated artifacts (charts, reports) per session
+-- uploads/                        Uploaded files (normalized to CSV)
```

---

## 16. Key Architectural Decisions

### Why does the coder have zero tools?

Before the restructure, the coder had 6 tools (validate, execute, chart gen, etc.) that were all pure Python — no LLM reasoning needed. The LLM was wasting API calls deciding to call tools that should just be called in sequence. Now the orchestrator calls those functions directly as Python, and only invokes the coder LLM when it genuinely needs code written.

### Why does the orchestrator do direct Python execution?

Because validation, execution, and quality checking are deterministic steps that don't need LLM judgment. Having the orchestrator call `validate_code()` → `execute_analysis()` → `validate_output_quality()` directly in Python is faster, cheaper, and more reliable than asking an LLM to decide to call those same functions.

### Why library functions instead of always using the LLM?

Pre-built library functions are:
- **Faster** — no LLM latency
- **Cheaper** — zero API cost
- **Deterministic** — same input = same output
- **Tested** — each function handles edge cases
- **Consistent** — standardized result envelope

The LLM coder is a fallback for truly novel analyses that don't have a library function yet.

### Why A2A messaging instead of direct function calls?

A2A messages create an audit trail. Every agent action is logged as a message with sender, recipient, intent, and payload. This makes debugging easy — you can trace exactly what happened at each step of the pipeline.

### Why is the discovery agent an LLM and not rule-based?

Discovery requires genuine reasoning: "Given this dataset has user IDs, timestamps, and event names, what analyses would produce the most valuable insights?" A rule-based system would either run everything (wasteful) or use rigid heuristics (misses opportunities). The LLM can also incorporate user instructions like "focus on dropout patterns" to customize the analysis plan.

### Why a standalone HTML report?

The report embeds all charts as iframes with `srcdoc`. This means:
- No server needed to view it
- Can be emailed or shared as a single file
- Works offline
- Charts are interactive (Plotly hover/zoom)

### Why progressive chart rendering?

Analysis can take minutes. Instead of showing a blank screen, the frontend polls every 2 seconds and renders charts as they complete. Users see results building up in real-time, which provides feedback that the system is working.

---

## 17. Deterministic Fallback System

The system is designed to **complete the full pipeline even when the LLM fails entirely**. Deterministic fallbacks ensure partial results are still produced if any agent times out or errors.

### Fallback Chain

```
LLM Agent Attempt              Deterministic Fallback
─────────────────              ──────────────────────
Orchestrator (max_turns=10)  → _fallback_pipeline()
  ├─ Profiler Agent          → tool_run_profiler() direct call
  ├─ Discovery Agent         → build_fallback_discovery() heuristic
  ├─ DAG Execution           → _execute_dag() with library functions
  ├─ Synthesis Agent         → Deterministic synthesis (4 tool calls)
  └─ Report Agent            → Direct _build_report_html() call
```

### How Each Fallback Works

**1. Discovery Fallback** (`main.py: build_fallback_discovery()`)
- Uses `csv_profiler.classify_dataset()` output: dataset_type + column_roles
- Calls `tool_build_analysis_dag()` as pure Python (uses `LIBRARY_REGISTRY` mapping)
- Picks from 15+ pre-built analysis types based on dataset classification
- Zero LLM involvement — fully heuristic

**2. DAG Execution Fallback** (`orchestrator.py: _execute_dag()`)
- Bypasses `tool_execute_dag()` tool wrapper (which has circular import issues in thread context)
- Calls `_execute_dag()` directly with the `state` object
- Each node: check `LIBRARY_REGISTRY` → `_build_library_call_code()` → `exec()` + `analyze(csv_path)`
- CSV header auto-detection when column_roles are all None (generic tabular data)
- Only falls back to coder LLM when NO library function exists (~5% of analyses)

**3. Synthesis Fallback** (`orchestrator.py: _fallback_pipeline()`)
- Builds `results_by_type` dict directly from `state.results`
- Calls 4 synthesis tools as plain Python functions:
  - `tool_generate_executive_summary()` — 5-7 bullet points with severity tagging
  - `tool_generate_segment_personas()` — named personas from DBSCAN clusters
  - `tool_generate_intervention_strategies()` — ranked strategies with root cause + action
  - `tool_generate_cross_metric_connections()` — cross-analysis correlations
- Stores result in `_synthesis_store` + `state.synthesis`

**4. Report Fallback** (`orchestrator.py: _fallback_pipeline()`)
- Collects chart HTML files from `state.results` and output folder
- Embeds each chart inline (reads HTML, escapes for srcdoc)
- Calls `_build_report_html()` directly — produces standalone HTML report
- Stores in `_report_store` + `state.artifacts`

### Max Turns Safety Valve

Each agent has a `max_turns` limit to prevent infinite loops:

| Agent | max_turns | Reason |
|---|---|---|
| Orchestrator | 10 | Prevents runaway coordination loops |
| Discovery | 12 | Limits exploration when LLM can't decide |
| Synthesis | 12 | Prevents repeated failed tool calls |
| Coder | 15 (default) | Allows multiple code generation attempts |
| Profiler | 15 (default) | Allows retry on classification |

When `max_turns` is hit, the agent stops and the fallback pipeline takes over.

### Thread Context Issues

The fallback pipeline runs in a `BackgroundTasks` thread via `asyncio.run()`. This creates issues with `from main import sessions` (circular import in thread context). The solution:
- `_fallback_pipeline()` receives `state` as a parameter
- Bypasses `tool_execute_dag()` and calls `_execute_dag()` directly
- Builds artifacts dict from `state.results` directly (no `_get_session_state()`)
- Synthesis and report functions that need `state` work directly with the passed object

---

## 18. Bug Fixes and Lessons Learned

### Critical Fix: DAG node status override
`_execute_single_node` returned `{"status": "success", **exec_result}`. If `exec_result` had its own `"status": "error"`, the spread would OVERWRITE `"success"` → causing `run_node_with_retry` to retry already-completed nodes. Fix: `{**exec_result, "status": "success"}` (spread first, then override).

### Critical Fix: Session not found in thread context
`tool_execute_dag()` calls `_get_session_state()` which does `from main import sessions`. This returns `None` in `BackgroundTasks` thread context. Fix: bypass `tool_execute_dag` entirely, call `_execute_dag()` directly with the `state` object.

### Critical Fix: state.status never set to "complete"
`_fallback_pipeline` returned `{"status": "complete"}` as a dict but never set `state.status = "complete"`. Frontend polls `state.status` via `/status` endpoint. Fix: explicitly set `state.status = "complete"` at end of fallback.

### Fix: Column roles all-None for generic datasets
For `tabular_generic` datasets, all column_roles are `None`. `_build_library_call_code` produced `col=""` → pandas crash. Fix: CSV header auto-detection — read 5 rows, pick first numeric/categorical column for each missing arg.

### Fix: A8 distribution analysis on non-numeric data
Event log datasets have no numeric columns (user_uuid, event_name, timestamp). Distribution analysis requires numeric data. The profiler recommends it anyway since it's a generic analysis. The pipeline handles this gracefully — quality check fails 3 times, submits with error status, pipeline continues with 9 good results.
