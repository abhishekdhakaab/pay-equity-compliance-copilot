# 🧮 Pay-Equity Compliance Copilot  
**Governed GenAI Agent on Databricks (Free-Tier Compatible)**

An **AI agent** that answers pay-equity questions — e.g.  
> “Are there any unexplained comp gaps by role or tenure?”  

— **without exposing PII**, using a **ReAct-style tool loop** backed by **Databricks views** and an **OpenRouter** LLM.

---

## 🚀 Key Features

- **Governed LLM Agent:** chooses and runs approved analysis tools only.  
- **PII-Safe:** queries go through masked / view-only layers.  
- **Fairness Analytics:** computes adjusted & unadjusted gender pay gaps, tenure and role breakdowns.  
- **ReAct Loop:** plan → act → observe → summarize.  
- **Free-Tier Friendly:** no Unity-Catalog masking or SQL functions required.  

---

## 🧱 Architecture Overview

### Data & Governance Layers
| Layer | Description |
|-------|--------------|
| **Raw tables** | `employees`, `jobs`, `compensation`, `performance`, `demographics` *(synthetic demo data)* |
| **Governed views** | `data_analyst_view`, `v_total_comp`, `v_with_tenure` — mask PII, exclude Legal dept |
| **Analysis views** | `v_unadjusted_paygap_gender`, `v_adjusted_gap_gender`, `v_equity_breakdown_*` |
| **Tools** | Python helpers: `unadjusted_gap_gender()`, `adjusted_gap_gender()`, `breakdown()` |
| **Agent** | LLM running on OpenRouter — plans → calls tools → observes → answers |

---

## 🧠 Why it’s a Real Agent

Unlike a simple “LLM-plus-SQL” script, this Copilot:

1. **Plans:** LLM outputs a JSON plan like  
   `{"tool": "adjusted_gap_gender", "args": {"job_family": "Engineering"}}`
2. **Acts:** Python executes that tool only (no raw SQL).
3. **Observes:** summarizes results (aggregates only, never PII).
4. **Iterates:** can call multiple tools before returning a final answer.
5. **Guards:** PII prompt detection and refusal logic.

---

## 🧩 Repository Layout

```
pay-equity-copilot/
├─ notebooks/
│  ├─ paygov_setup.py              # create catalog/schema + demo tables
│  ├─ paygov_views.py              # create analysis views
│  ├─ paygov_tools_python.py       # governed helper functions
│  └─ paygov_agent.py              # ReAct agent using OpenRouter
├─ data/                           # (optional synthetic CSVs)
├─ screenshots/                    # (optional UI screenshots)
├─ .gitignore
├─ README.md
└─ LICENSE
```

---

## 🧰 Prerequisites

- **Databricks Free Community edition** account  
- **OpenRouter API key** → [openrouter.ai](https://openrouter.ai)  
- **Python 3.10+** (if running locally)
- Basic familiarity with Databricks notebooks

---

## 🪜 Step-by-Step Workflow (Full Runbook)

### 1️⃣ Create a Cluster
- Launch Databricks → **Compute → Create Cluster**
- Attach all notebooks to this cluster.

### 2️⃣ Create Catalog and Schema
Open `notebooks/paygov_setup.py` and run each cell sequentially:
- Creates catalog `paygov`
- Creates schema `hr_data`
- Loads five small synthetic tables:
  - `employees`, `jobs`, `compensation`, `performance`, `demographics`

### 3️⃣ Create Safe Analysis Views
Run `notebooks/paygov_views.py`:
- Builds `v_total_comp` and `v_with_tenure`
- Adds pre-aggregated fairness views:
  - `v_unadjusted_paygap_gender`
  - `v_adjusted_gap_gender`
  - `v_equity_breakdown_tenure`, `v_equity_breakdown_role`, `v_equity_breakdown_location`

### 4️⃣ Add Python Tools
Run `notebooks/paygov_tools_python.py`:
- Defines:
  - `unadjusted_gap_gender(job_family, job_level, location)`
  - `adjusted_gap_gender(job_family, location)`
  - `breakdown(by, job_family)`

All tools read **views**, not raw tables.

### 5️⃣ Configure LLM Agent
Open `notebooks/paygov_agent.py`:
1. Insert your OpenRouter API key  
   ```python
   client = OpenAI(
       base_url="https://openrouter.ai/api/v1",
       api_key=os.environ["OPENROUTER_API_KEY"]
   )
   ```
2. Run cells to load:
   - tool registry  
   - PII guard  
   - summarizer  
   - ReAct agent loop (`run_agent()`)

### 6️⃣ Test the Copilot
```python
run_agent("Show me the adjusted gender pay gap after controlling for level and tenure.")
run_agent("Break down compensation by tenure for Engineering.")
run_agent("Give me the unadjusted gender pay gap overall.")
run_agent("What is Dan Kim's salary?")  # should safely refuse
```

Expected output:
```
🤖 Copilot plan → executes tool → shows safe preview
🧾 Final Summary: After controlling for level & tenure, female/male ratio ≈ 0.96
```

---

## 🧱 Governance Mapping

| Governance Pillar | Implementation |
|--------------------|----------------|
| **Lifecycle / Separation of Duties** | Views-only tools; notebooks version-controlled |
| **Risk Management / Defense in Depth** | PII masked in views; Legal dept excluded |
| **Security / Least Privilege** | Agent can only call whitelisted tools |
| **Observability / Audit** | (Extendable) log `(question, tool, args, rollup, final)` to Delta table |

---

## 🔐 Secrets & Safety

Create `.env` file (ignored via `.gitignore`):
```
OPENROUTER_API_KEY=sk-or-xxxxxxxx
```

Never commit your key.  
The agent refuses any query containing patterns like `ssn`, `email`, `who is`, or `top paid`.

---

## 📈 Future Extensions

- **Audit trail:** log all runs to Delta table  
- **Quarterly summaries:** auto-generate compliance PDFs  
- **Fine-tuned models:** fairness-aware summarization  
- **Manager-specific access:** via on-behalf-of auth  
- **Evaluation sets:** correctness + leakage red-team prompts  

---

## ⚙️ Local Development (Optional)

You can test outside Databricks with small CSVs:
```bash
pip install pyspark openai pandas
```
Edit `paygov_tools_python.py` to load local CSVs instead of Spark tables.

---

## 🧩 Repo Setup

1. Clone your repo:
   ```bash
   git clone https://github.com/<YOUR_USERNAME>/pay-equity-copilot.git
   cd pay-equity-copilot
   ```
2. Add notebooks:
   - Export each notebook from Databricks as `.py`
   - Place them under `/notebooks`
3. Commit & push:
   ```bash
   git add .
   git commit -m "Initial commit of Pay-Equity Copilot"
   git push origin main
   ```

---

## ⚖️ License
MIT License © 2025 Your Name
