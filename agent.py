import asyncio
import uuid
import warnings
import os
from google.adk.agents import Agent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools import FunctionTool
from google.genai import types
from google.cloud import spanner

# --- Configuration & Suppression ---
warnings.filterwarnings("ignore", category=UserWarning, module="google_genai")
os.environ["GOOGLE_CLOUD_SPANNER_ENABLE_METRICS"] = "false"
os.environ["OTEL_SDK_DISABLED"] = "true"

PROJECT_ID = "bq-project-402513"
INSTANCE_ID = "graph"
DATABASE_ID = "supportgraph"

# Initialize Spanner
spanner_client = spanner.Client()
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)

# --- 2. Tool Functions ---

def check_retention_history(customer_id: str):
    """Lookup historical decisions and outcomes from the Spanner Context Graph."""
    gql_query = f"GRAPH SupportContextGraph MATCH (c:Customers {{customer_id: '{customer_id}'}})<-[:AboutCustomer]-(d:Decisions) MATCH (d)-[:ResultedIn]->(o:Outcomes) RETURN d.timestamp, d.type, d.amount, o.result ORDER BY d.timestamp DESC"
    with database.snapshot() as snapshot:
        rows = list(snapshot.execute_sql(gql_query))
        return [{"date": str(r[0]), "type": r[1], "amt": r[2], "result": r[3]} for r in rows]

def get_policy_details(policy_id: str):
    """Retrieves the active corporate rule definition from the Policies table."""
    sql = "SELECT name, rule_definition FROM Policies WHERE policy_id = @pid"
    with database.snapshot() as snapshot:
        rows = list(snapshot.execute_sql(sql, params={'pid': policy_id}, param_types={'pid': spanner.param_types.STRING}))
        return {"name": rows[0][0], "rule": rows[0][1]} if rows else "Not found"

def log_agent_decision(customer_id: str, policy_id: str, decision_type: str, reasoning: str):
    """Codifies the Agent's reasoning back into the Spanner Decisions and Edge tables."""
    new_id = f"DEC-{uuid.uuid4().hex[:6].upper()}"
    
    def insert_decision(transaction):
        # 1. Insert the Decision Node
        transaction.execute_update(
            "INSERT INTO Decisions (decision_id, type, reasoning_text, timestamp) VALUES (@id, @type, @reason, CURRENT_TIMESTAMP())",
            params={'id': new_id, 'type': decision_type, 'reason': reasoning},
            param_types={'id': spanner.param_types.STRING, 'type': spanner.param_types.STRING, 'reason': spanner.param_types.STRING}
        )
        # 2. Create the Edges (Connections)
        transaction.execute_update("INSERT INTO AboutCustomer (decision_id, customer_id) VALUES (@d, @c)", params={'d': new_id, 'c': customer_id}, param_types={'d': spanner.param_types.STRING, 'c': spanner.param_types.STRING})
        transaction.execute_update("INSERT INTO FollowedPolicy (decision_id, policy_id) VALUES (@d, @p)", params={'d': new_id, 'p': policy_id}, param_types={'d': spanner.param_types.STRING, 'p': spanner.param_types.STRING})

    database.run_in_transaction(insert_decision)
    return f"Successfully codified Decision {new_id} into Institutional Memory."

# --- 3. ADK Setup ---

tools = [
    FunctionTool(check_retention_history),
    FunctionTool(get_policy_details),
    FunctionTool(log_agent_decision) # New Tool added here
]

async def run_governed_pipeline(text_input: str):
    report_instruction = (
        "You are an expert retention strategist. Follow these steps:\n"
        "1. Run 'check_retention_history'.\n"
        "2. IF a failure is found, run 'get_policy_details' for 'POL-99'.\n"
        "3. Present the '🔍 CONTEXT GRAPH INTELLIGENCE REPORT'.\n"
        "4. MANDATORY: Call 'log_agent_decision' to save your reasoning to Spanner before finishing."
    )

    agent = Agent(name="retention_specialist", model="gemini-2.0-flash", instruction=report_instruction, tools=tools)
    session_service = InMemorySessionService()
    await session_service.create_session(app_name="RetentionApp", user_id="user_123", session_id="session_final")
    runner = Runner(app_name="RetentionApp", agent=agent, session_service=session_service)

    async for event in runner.run_async(new_message=types.Content(role='user', parts=[types.Part(text=text_input)]), user_id="user_123", session_id="session_final"):
        if event.author and event.content and event.content.parts:
            print(f"\n[{event.author}]: {event.content.parts[0].text}")

if __name__ == "__main__":
    asyncio.run(run_governed_pipeline("Should I give CUST-001 a 50% discount?"))
