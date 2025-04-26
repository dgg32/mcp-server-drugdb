# server.py
import duckdb
import openai
import yaml
from mcp.server.fastmcp import FastMCP

# Create an MCP server
mcp = FastMCP("DrugDB")

with open("config.yaml", "r") as stream:
    try:
        PARAM = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)



@mcp.tool()
def query_data(sql: str) -> str:
    """Execute SQL queries safely"""
    con = duckdb.connect("drug.db")

    con.install_extension("duckpgq", repository="community")

    con.sql("INSTALL fts;")
    con.sql("INSTALL vss;")

    con.load_extension("duckpgq")
    con.load_extension("fts")
    con.load_extension("vss")

    openai.api_key  = PARAM['openai_api']
    client = openai.OpenAI(api_key = PARAM['openai_api'])

    def embeddings(text) -> list[float]:
        text = text.replace("\n", " ")
        return client.embeddings.create(input = [text], model='text-embedding-3-small').data[0].embedding

    con.create_function('embeddings', embeddings)

    try:
        result = con.sql(sql).fetchall()
        con.commit()
        return "\n".join(str(row) for row in result)
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        con.close()


@mcp.prompt()
def drugdb_prompt() -> str:
    """
    Provide examples and schema information for the drug database
    to help Claude write better SQL queries.
    """
    return """
# DrugDB Database Information and Query Examples

## Database Schema
The database contains the following main tables:
- Trials: Contains information clinical trias (id, Sponsor, name, Disorder, drug_id, drug_name, Phase, LinkToSponsorStudyRegistry, LinkToClinicalTrials). Disorder is claimed by the sponsor. drug_id is a list of drug ids, which correspond to the id in the Drug table. Use drug_id to connect to the Drug table.
- Drug: Contains drug data (id, name, label). label is always "Drug" and is in the knowledge graph, the name is the drug name.
- Disorder: Contains disorder data (id, name, label, definition, definitionEmbedding). label is always "Disorder" and is in the knowledge graph, the name is the disorder name. The definition is the disorder definition, and it can be search via full-text search.
- MOA: Contains mechanisms of action data (id, name, label). label is always "MOA" and is in the knowledge graph, the name is the MOA name.
- Prescription: Contains prescription data (id, name, label, prescription_cost_over_time, prescriptions_over_time, cost_per_day_over_time, and other columns). label is always "Prescription" and is in the knowledge graph, the name is the prescription name.
- DrugDisorder: Contains the MAY_TREAT relationship between drugs and disorders (from_id, to_id, label). from_id is the id of the drug in Drug table, and to_id is the id of the disorder in the Disorder table. label is always "MAY_TREAT" and is in the knowledge graph.
- DrugMOA: Contains the HAS_MOA relationship between drugs and mechanisms of action (from_id, to_id, label). from_id is the id of the drug in Drug table, and to_id is the id of the MOA in the MOA table. label is always "HAS_MOA" and is in the knowledge graph.
- PrescriptionRelatedDrugs: Contains the RELATED relationship between prescriptions (from_id, to_id, label). from_id is the id of one prescription in Prescription table, and to_id is the id of the another prescription in Prescription table. label is always "RELATED" and is in the knowledge graph.
- PrescriptionSubstance: Contains the CONTAINS_SUBSTANCE relationship between prescriptions and drugs (from_id, to_id, label). from_id is the id of the prescription in Prescription table, and to_id is the id of the drug in Drug table. label is always "CONTAINS_SUBSTANCE" and is in the knowledge graph.
- DrugDrugInteraction: Contains the INTERACTS_WITH relationship between drugs (from_id, to_id, label). from_id is the id of one drug in Drug table, and to_id is the id of another drug in Drug table. label is always "INTERACTS_WITH" and is in the knowledge graph.

### If you use getCanonicalNameAndCUI before the query, you can get the canonical name and CUI of the medical terms. Try to use the CUI to query the database first. If that doesn't work, use the canonical name.

### Some questions can be answered using the graph structure of the database or pure SQL queries.

## Example Queries

```sql
-- Get all other drugs that share any of the MOA and may treat the any of the disorders as "pembrolizumab" 
SELECT DISTINCT c.name AS similar_drug
FROM GRAPH_TABLE (drug_graph MATCH (p:MOA)<-[q:HAS_MOA]-(a:Drug WHERE a.name='pembrolizumab')-[n:MAY_TREAT]->(b:Disorder)<-[o:MAY_TREAT]-(c:Drug WHERE c.name != 'pembrolizumab')-[r:HAS_MOA]->(p:MOA) COLUMNS (a, n, b, o, c, r, p, q))

-- For this question above, you can use the following SQL query as well:
WITH pembro AS (SELECT id FROM Drug WHERE name = 'pembrolizumab'), pembro_moas AS (SELECT to_id FROM DrugMOA WHERE from_id = (SELECT id FROM pembro)), pembro_disorders AS (SELECT to_id FROM DrugDisorder WHERE from_id = (SELECT id FROM pembro)) SELECT DISTINCT d.id, d.name FROM Drug d WHERE EXISTS (SELECT 1 FROM DrugMOA dm WHERE dm.from_id = d.id AND dm.to_id IN (SELECT to_id FROM pembro_moas)) AND EXISTS (SELECT 1 FROM DrugDisorder dd WHERE dd.from_id = d.id AND dd.to_id IN (SELECT to_id FROM pembro_disorders)) AND d.name != 'pembrolizumab' ORDER BY d.name;

-- Show 5 trials and their drugs. At least one of the drugs must be used to treat against the Non-Small Cell Lung Carcinoma?
SELECT name, drug_id
         FROM Trials,
         GRAPH_TABLE (drug_graph MATCH (a:Drug)-[n:MAY_TREAT]->(b:Disorder WHERE b.name='Non-Small Cell Lung Carcinoma') COLUMNS (a.id AS drug_cui)) drug_for_disease
         WHERE list_contains(Trials.drug_id, drug_for_disease.drug_cui)        
         LIMIT 5;


-- Which substance that may treat Diabetes Mellitus, Insulin-Dependent but does not have the MOA Insulin Receptor Agonists
SELECT DISTINCT a.name AS drug_name
FROM GRAPH_TABLE (drug_graph MATCH (p:MOA WHERE p.name != 'Insulin Receptor Agonists')<-[q:HAS_MOA]-(a:Drug)-[n:MAY_TREAT]->(b:Disorder WHERE b.name = 'Diabetes Mellitus, Insulin-Dependent') COLUMNS (p, q, a, n ,b))

-- For this question above, you can use the following SQL query as well:
SELECT d.id, d.name FROM Drug d JOIN DrugDisorder dd ON d.id = dd.from_id JOIN Disorder dis ON dd.to_id = dis.id WHERE dis.name = 'Diabetes Mellitus, Non-Insulin-Dependent' AND d.id NOT IN (SELECT dm.from_id FROM DrugMOA dm JOIN MOA m ON dm.to_id = m.id WHERE m.name = 'Insulin Receptor Agonists');

-- Give me the top 5 most prescribed drugs for the year 2020
SELECT Pre.name, list_filter(Pre.prescriptions_over_time, x -> x.year = 2020)[1].total_prescriptions AS total_prescriptions 
FROM Prescription Pre ORDER BY total_prescriptions DESC LIMIT 5;

-- Give me the top 5 most prescribed drugs for the year 2022, and show their amounts of prescriptions for 2020, 2021, and 2022
SELECT Pre.name, list_filter(Pre.prescriptions_over_time, x -> x.year = 2020)[1].total_prescriptions AS total_prescriptions_2020, list_filter(Pre.prescriptions_over_time, x -> x.year = 2021)[1].total_prescriptions AS total_prescriptions_2021,  list_filter(Pre.prescriptions_over_time, x -> x.year = 2022)[1].total_prescriptions AS total_prescriptions_2022 
FROM Prescription Pre ORDER BY total_prescriptions_2022 DESC LIMIT 5;

-- Give me three joint-related disorders
-- Because the user used the word "related", it is better to use the vector search tool to go through the disorder definition embedding to find the disorders
SELECT name, definition
        FROM Disorder
        ORDER BY array_distance(definitionEmbedding, embeddings('joint-related disorders')::FLOAT[1536])
        LIMIT 3;

-- Give me the top 10 most prescribed drugs for the year 2022, and then show their substance and substances' indications
SELECT Pre.name, list_filter(prescriptions_over_time, x -> x.year = 2022)[1].total_prescriptions AS total_prescriptions, list(DISTINCT d.name), list(DISTINCT dis.name) 
FROM Prescription Pre JOIN PrescriptionSubstance PS ON Pre.id = PS.from_id JOIN Drug d ON d.id = PS.to_id JOIN DrugDisorder DD ON d.id = DD.from_id JOIN Disorder dis ON dis.id = DD.to_id GROUP BY Pre.name, total_prescriptions ORDER BY total_prescriptions DESC LIMIT 10;


-- Give me the top 10 most prescribed drugs for the year 2022, and then show their substance and the substances' MOA
SELECT Pre.name, list_filter(prescriptions_over_time, x -> x.year = 2022)[1].total_prescriptions AS total_prescriptions, list(DISTINCT d.name), list(DISTINCT m.name)  FROM Prescription Pre JOIN PrescriptionSubstance PS ON Pre.id = PS.from_id JOIN Drug d ON d.id = PS.to_id JOIN DrugMOA DM ON d.id = DM.from_id JOIN MOA m ON m.id = DM.to_id GROUP BY Pre.name, total_prescriptions ORDER BY total_prescriptions DESC LIMIT 10;


-- Give me the top 5 most expensive drugs (total cost) for the year 2022 and also show their out-of-pocket cost
SELECT Pre.name, list_filter(Pre.prescription_cost_over_time, x -> x.year = 2022)[1].total AS total_cost, list_filter(Pre.prescription_cost_over_time, x -> x.year = 2022)[1].out_of_pocket AS out_of_pocket FROM Prescription Pre ORDER BY total_cost DESC LIMIT 5;

```

### Error Prevention Tips
1. Always use parameterized queries when working with user input
2. Use LIMIT clauses when exploring large tables
3. Check if tables exist before querying them
4. Be aware that some extensions (fts, vss, duckpgq) require specific syntax
5. For vector searches, normalize vectors before comparison

When writing SQL, always consider the specific database schema and available extensions.
"""


if __name__ == "__main__":
    print("Starting server...")
    # Initialize and run the server

    mcp.run(transport="stdio")