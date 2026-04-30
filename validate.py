import os
import json
import sqlparse

def validate():
    required_files = [
        'schema.sql',
        'slow_queries.sql',
        'schema_analysis.json',
        'query_diagnoses.json',
        'optimised_queries.sql',
        'index_plan.sql',
        'index_deduplication.json',
        'schema_improvement_plan.md',
        'llm_calls.jsonl'
    ]
    
    # Check all files exist
    for file in required_files:
        if not os.path.exists(file):
            print(f"ERROR: {file} does not exist")
            return False
    
    # Check JSON files are valid
    json_files = ['schema_analysis.json', 'query_diagnoses.json', 'index_deduplication.json']
    for file in json_files:
        try:
            with open(file) as f:
                json.load(f)
        except:
            print(f"ERROR: {file} is not valid JSON")
            return False
    
    # Check llm_calls.jsonl
    try:
        with open('llm_calls.jsonl') as f:
            for line in f:
                json.loads(line.strip())
    except:
        print("ERROR: llm_calls.jsonl is not valid JSONL")
        return False
    
    # Check schema.sql and slow_queries.sql are readable
    try:
        with open('schema.sql') as f:
            sqlparse.parse(f.read())
    except:
        print("ERROR: schema.sql is not valid SQL")
        return False
    
    try:
        with open('slow_queries.sql') as f:
            content = f.read()
            # Basic check
            if not content.strip():
                raise ValueError("Empty")
    except:
        print("ERROR: slow_queries.sql is not readable")
        return False
    
    # Check optimised_queries.sql and index_plan.sql are non-empty
    for file in ['optimised_queries.sql', 'index_plan.sql']:
        try:
            with open(file) as f:
                if not f.read().strip():
                    print(f"ERROR: {file} is empty")
                    return False
        except:
            print(f"ERROR: {file} not readable")
            return False
    
    # Check each query has diagnosis and rewrite
    with open('query_diagnoses.json') as f:
        diagnoses = json.load(f)
    with open('slow_queries.sql') as f:
        queries_content = f.read()
    queries = queries_content.split('-- Q')[1:]
    if len(diagnoses) != len(queries):
        print("ERROR: Number of diagnoses doesn't match number of queries")
        return False
    
    with open('optimised_queries.sql') as f:
        rewrites_content = f.read()
    for i in range(len(queries)):
        query_id = f"Q{i+1}"
        if f"-- {query_id}" not in rewrites_content:
            print(f"ERROR: Rewrite for {query_id} not found")
            return False
    
    # Check index deduplication
    with open('index_deduplication.json') as f:
        dedup = json.load(f)
    if 'unique_indexes' not in dedup:
        print("ERROR: index_deduplication.json missing unique_indexes")
        return False
    
    # Check llm_calls has required entries
    stages = set()
    with open('llm_calls.jsonl') as f:
        for line in f:
            entry = json.loads(line.strip())
            stages.add(entry['stage'])
    required_stages = {'schema_analysis', 'query_diagnosis', 'query_rewrite', 'index_suggestion'}
    if not required_stages.issubset(stages):
        print(f"ERROR: Missing LLM stages: {required_stages - stages}")
        return False
    
    print("All validations passed!")
    return True

if __name__ == "__main__":
    validate()