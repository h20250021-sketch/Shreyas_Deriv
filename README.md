# PostgreSQL Performance Analysis Pipeline

This pipeline analyzes a PostgreSQL database schema and slow-running SQL queries to diagnose performance issues, suggest query rewrites, and recommend indexes.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up OpenAI API key in `.env`:
   ```
   OPENAI_API_KEY=your_api_key_here
   ```

## Usage

Run the pipeline:
```bash
python pipeline.py
```

The pipeline will:
- Parse `schema.sql` and `slow_queries.sql`
- Use staged LLM reasoning to analyze schema and each query
- Generate diagnoses, rewrites, and index recommendations
- Deduplicate index suggestions deterministically
- Produce all required output files

## Validation

Run validation to check all outputs:
```bash
python validate.py
```

## Outputs

- `schema_analysis.json`: Analysis of the database schema
- `query_diagnoses.json`: Performance issues for each query
- `optimised_queries.sql`: Suggested query rewrites
- `index_plan.sql`: Recommended indexes to add
- `index_deduplication.json`: Deduplicated index suggestions
- `schema_improvement_plan.md`: Consolidated improvement plan
- `llm_calls.jsonl`: Log of all LLM calls made during the pipeline

## Pipeline Stages

1. **Schema Analysis**: Analyze schema structure and existing indexes
2. **Query Diagnoses**: Diagnose performance issues for each slow query
3. **Optimized Queries**: Generate rewritten queries for better performance
4. **Index Plan**: Suggest indexes and deduplicate them
5. **Schema Improvement Plan**: Produce a markdown plan with all recommendations

## Notes

- This is static analysis; no database connection required
- Outputs are reproducible with temperature=0 LLM calls
- Pipeline preserves intermediate artifacts for replayability
- All artifacts are regenerated on each run