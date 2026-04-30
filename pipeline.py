import os
import json
import sqlparse
import hashlib
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def log_llm_call(stage, query_id, provider, model, prompt, input_artifacts, output_artifact):
    entry = {
        "stage": stage,
        "query_id": query_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "provider": provider,
        "model": model,
        "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest(),
        "input_artifacts": input_artifacts,
        "output_artifact": output_artifact
    }
    with open('llm_calls.jsonl', 'a') as f:
        f.write(json.dumps(entry) + '\n')

def parse_schema(schema_file):
    with open(schema_file, 'r') as f:
        content = f.read()
    
    parsed = sqlparse.parse(content)
    tables = {}
    indexes = []
    
    for stmt in parsed:
        sql_upper = str(stmt).strip().upper()
        if sql_upper.startswith('CREATE TABLE'):
            # Parse table
            lines = [line.strip() for line in str(stmt).split('\n') if line.strip()]
            table_name = None
            columns = []
            in_columns = False
            for line in lines:
                if line.upper().startswith('CREATE TABLE'):
                    table_name = line.split()[2].strip('(').strip(')')
                elif line == '(':
                    in_columns = True
                elif line == ');':
                    break
                elif in_columns and not line.startswith('--'):
                    if ',' in line or line.upper().startswith('PRIMARY KEY') or 'REFERENCES' in line.upper():
                        continue
                    parts = line.split()
                    if parts:
                        col_name = parts[0]
                        col_type = ' '.join(parts[1:])
                        columns.append((col_name, col_type))
            if table_name:
                tables[table_name] = columns
        elif sql_upper.startswith('CREATE INDEX'):
            # Parse index
            parts = str(stmt).split()
            index_name = parts[2]
            on_idx = parts.index('ON')
            table_name = parts[on_idx + 1]
            columns_part = ' '.join(parts[on_idx + 2:])
            columns = [col.strip('()') for col in columns_part.split(',')]
            indexes.append((index_name, table_name, columns))
    
    return tables, indexes

def parse_queries(queries_file):
    with open(queries_file, 'r') as f:
        content = f.read()
    
    # Split by -- Qn:
    queries = []
    parts = content.split('-- Q')
    for part in parts[1:]:
        query = part.split('\n', 1)[1].strip()
        queries.append(query)
    
    return queries

def call_llm(prompt, stage, query_id=None, input_artifacts=None, output_artifact=None):
    if not client.api_key:
        # Mock responses for testing
        if stage == "schema_analysis":
            return '{"analysis": "Schema has users, accounts, positions, transactions, audit_log tables with some indexes"}'
        elif stage == "query_diagnosis":
            return '{"issues": ["Missing index on last_login for date filtering", "Large table scan on users"], "severity": "high"}'
        elif stage == "query_rewrite":
            return '{"rewrite": "SELECT DATE(last_login) as login_date, country_code, COUNT(*) as dau FROM users WHERE last_login >= NOW() - INTERVAL \'30 days\' GROUP BY DATE(last_login), country_code ORDER BY login_date DESC, dau DESC;"}'
        elif stage == "index_suggestion":
            return '{"indexes": ["CREATE INDEX idx_users_last_login ON users(last_login);", "CREATE INDEX idx_positions_status ON positions(status);"]}'
        else:
            return '{"mock": "response"}'
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    result = response.choices[0].message.content
    log_llm_call(stage, query_id, "openai", "gpt-4", prompt, input_artifacts or [], output_artifact or "")
    return result

def stage1_schema_analysis(schema_file):
    tables, indexes = parse_schema(schema_file)
    schema_str = open(schema_file).read()
    prompt = f"""
Analyze this PostgreSQL schema for performance optimization opportunities:

{schema_str}

Output in JSON format:
{{
  "tables": {{"table_name": [["col", "type"], ...]}},
  "existing_indexes": [["index_name", "table", ["cols"]]],
  "analysis": "summary of schema structure and potential issues"
}}
"""
    response = call_llm(prompt, "schema_analysis", input_artifacts=[schema_file], output_artifact="schema_analysis.json")
    analysis = json.loads(response)
    analysis["tables"] = tables
    analysis["existing_indexes"] = indexes
    with open('schema_analysis.json', 'w') as f:
        json.dump(analysis, f, indent=2)

def stage2_query_diagnoses(schema_file, queries_file):
    with open('schema_analysis.json') as f:
        schema_analysis = json.load(f)
    schema_str = json.dumps(schema_analysis, indent=2)
    queries = parse_queries(queries_file)
    diagnoses = []
    for i, query in enumerate(queries):
        query_id = f"Q{i+1}"
        prompt = f"""
Given the PostgreSQL schema analysis:
{schema_str}

And the slow query:
{query}

Diagnose likely performance issues. Focus on missing indexes, inefficient joins, large scans, etc.
Output in JSON format: {{"issues": ["issue1", "issue2"], "severity": "high|medium|low"}}
"""
        response = call_llm(prompt, "query_diagnosis", query_id, [schema_file, queries_file], "query_diagnoses.json")
        diag = json.loads(response)
        diag["query_id"] = query_id
        diagnoses.append(diag)
    with open('query_diagnoses.json', 'w') as f:
        json.dump(diagnoses, f, indent=2)

def stage3_optimised_queries(schema_file, queries_file):
    with open('schema_analysis.json') as f:
        schema_analysis = json.load(f)
    schema_str = json.dumps(schema_analysis, indent=2)
    with open('query_diagnoses.json') as f:
        diagnoses = json.load(f)
    queries = parse_queries(queries_file)
    rewrites = []
    sql_content = ""
    for i, (query, diag) in enumerate(zip(queries, diagnoses)):
        query_id = f"Q{i+1}"
        diag_str = json.dumps(diag, indent=2)
        prompt = f"""
Given the schema:
{schema_str}

Slow query:
{query}

Diagnosis:
{diag_str}

Suggest a rewritten query to improve performance.
Output in JSON format: {{"rewrite": "SELECT ..."}}
"""
        response = call_llm(prompt, "query_rewrite", query_id, ["schema_analysis.json", "query_diagnoses.json", queries_file], "optimised_queries.sql")
        rew = json.loads(response)
        rewrites.append({"query_id": query_id, "original": query, "rewrite": rew["rewrite"]})
        sql_content += f"-- {query_id} rewrite\n{rew['rewrite']}\n\n"
    with open('optimised_queries.sql', 'w') as f:
        f.write(sql_content)

def stage4_index_plan():
    with open('query_diagnoses.json') as f:
        diagnoses = json.load(f)
    with open('schema_analysis.json') as f:
        schema_analysis = json.load(f)
    schema_str = json.dumps(schema_analysis, indent=2)
    with open('optimised_queries.sql') as f:
        rewrites_content = f.read()
    queries = parse_queries('slow_queries.sql')
    index_suggestions = []
    for i, (query, diag) in enumerate(zip(queries, diagnoses)):
        query_id = f"Q{i+1}"
        diag_str = json.dumps(diag, indent=2)
        rewrite = rewrites_content.split(f'-- {query_id}')[1].split('--')[0].strip() if f'-- {query_id}' in rewrites_content else ""
        prompt = f"""
Given the schema:
{schema_str}

Original query:
{query}

Diagnosis:
{diag_str}

Rewrite:
{rewrite}

Suggest indexes to add for performance.
Output in JSON format: {{"indexes": ["CREATE INDEX ...", ...]}}
"""
        response = call_llm(prompt, "index_suggestion", query_id, ["schema_analysis.json", "query_diagnoses.json", "optimised_queries.sql"], "index_plan.sql")
        idx = json.loads(response)
        index_suggestions.extend(idx["indexes"])
    # Deduplicate
    unique_indexes = list(set(index_suggestions))
    unique_indexes.sort()
    dedup = {"original_count": len(index_suggestions), "unique_indexes": unique_indexes}
    with open('index_deduplication.json', 'w') as f:
        json.dump(dedup, f, indent=2)
    # Log dedup if LLM-assisted, but here it's static
    sql_content = "\n".join(unique_indexes)
    with open('index_plan.sql', 'w') as f:
        f.write(sql_content)

def stage5_schema_improvement_plan():
    with open('schema_analysis.json') as f:
        schema = json.load(f)
    with open('query_diagnoses.json') as f:
        diagnoses = json.load(f)
    with open('index_deduplication.json') as f:
        dedup = json.load(f)
    plan = f"""
# Schema Improvement Plan

## Existing Schema Analysis
{json.dumps(schema, indent=2)}

## Query Diagnoses
{json.dumps(diagnoses, indent=2)}

## Recommended Indexes
{chr(10).join(dedup['unique_indexes'])}

## Implementation Steps
1. Add the recommended indexes to schema.sql
2. Test the optimized queries in optimised_queries.sql
3. Monitor performance improvements
"""
    with open('schema_improvement_plan.md', 'w') as f:
        f.write(plan)

def main():
    # Clean previous outputs
    for file in ['schema_analysis.json', 'query_diagnoses.json', 'optimised_queries.sql', 'index_plan.sql', 'index_deduplication.json', 'schema_improvement_plan.md', 'llm_calls.jsonl']:
        if os.path.exists(file):
            os.remove(file)
    
    schema_file = 'schema.sql'
    queries_file = 'slow_queries.sql'
    
    print("Stage 1: Schema Analysis")
    stage1_schema_analysis(schema_file)
    
    print("Stage 2: Query Diagnoses")
    stage2_query_diagnoses(schema_file, queries_file)
    
    print("Stage 3: Optimized Queries")
    stage3_optimised_queries(schema_file, queries_file)
    
    print("Stage 4: Index Plan")
    stage4_index_plan()
    
    print("Stage 5: Schema Improvement Plan")
    stage5_schema_improvement_plan()
    
    print("Pipeline completed.")

if __name__ == "__main__":
    main()