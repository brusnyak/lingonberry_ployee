"""
Pydantic AI Agent with Step 3.5 Flash
Fast, type-safe agent implementation replacing slow MCP
"""

from pydantic_ai import Agent
from pydantic import BaseModel, Field
from typing import Optional, Literal, List
import asyncio
import os
import subprocess
import json
from datetime import datetime

# ============================================================================
# Configuration - Step 3.5 Flash (free) from OpenRouter
# ============================================================================

# Get API key from environment
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")

# Model configuration - Step 3.5 Flash (free tier, good context window)
DEFAULT_MODEL = "openrouter:stepfun/step-3.5-flash"
# Fallback options if needed:
# DEFAULT_MODEL = "openrouter:nvidia/llama-3.1-nemotron-70b-instruct"  # Alternative free option

# ============================================================================
# Structured Output Models
# ============================================================================

class ReplyClassification(BaseModel):
    """Classification result for email replies"""
    intent: Literal["interested", "question", "not_interested", "ignore", "unsubscribe"] = Field(
        description="Primary intent of the reply"
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="Confidence score for the classification"
    )
    urgency: Literal["high", "medium", "low"] = Field(
        description="Urgency level based on tone and content"
    )
    next_action: Literal["draft_reply", "schedule_call", "ignore", "forward"] = Field(
        description="Recommended next action"
    )
    draft_subject: Optional[str] = Field(
        None, description="Suggested subject line if drafting reply"
    )
    key_points: List[str] = Field(
        default_factory=list,
        description="Key points extracted from the reply"
    )


class DraftGeneration(BaseModel):
    """Generated email draft result"""
    subject: str = Field(description="Email subject line")
    body: str = Field(description="Email body content")
    tone: Literal["friendly", "professional", "casual", "formal"] = Field(
        description="Detected tone of the draft"
    )
    word_count: int = Field(description="Word count of the body")
    follow_up_timing: Optional[str] = Field(
        None, description="Recommended follow-up timing if no reply"
    )


class LeadEnrichment(BaseModel):
    """Enrichment data extracted from lead research"""
    contact_name: Optional[str] = Field(None, description="First name of contact")
    pain_detected: Optional[str] = Field(None, description="Detected pain point")
    gap_primary: Optional[str] = Field(None, description="Primary gap/opportunity")
    outreach_angle: Optional[str] = Field(None, description="Specific angle for outreach")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence in enrichment")


class ResearchOutput(BaseModel):
    """Structured research output"""
    query: str
    summary: str
    key_findings: List[str]
    sources: List[str]
    confidence: float
    suggested_actions: List[str]
    structured_data: Optional[dict] = None  # For CSV/JSON export


class ClientReport(BaseModel):
    """Client report for chat"""
    period: str
    replies_received: int
    replies_handled: int
    conversions: int
    pipeline_value: str
    urgent_items: List[str]
    daily_summary: str


# ============================================================================
# Agent Definitions
# ============================================================================

# Reply Classifier Agent (Step 3.5 Flash)
classifier_agent = Agent(
    model=DEFAULT_MODEL,
    output_type=ReplyClassification,
    system_prompt="""You classify email replies from Australian trades business leads (plumbers, electricians).

Categories:
- interested: wants to buy/book, asks about pricing, requests quote
- question: asking for info, clarification, or details  
- not_interested: declined, not now, no budget, competitor chosen
- ignore: spam, auto-replies, bounced emails
- unsubscribe: explicitly wants off the list

Assess urgency based on tone. Extract key points.""",
    model_settings={"temperature": 0.1}
)

# Draft Generator Agent (Step 3.5 Flash)
draft_agent = Agent(
    model=DEFAULT_MODEL,
    output_type=DraftGeneration,
    system_prompt="""Draft outreach emails for Australian trades businesses.

Voice: Honest, direct, human. Not salesy.

Rules:
- Under 60 words, plain text
- One simple question at the end
- Never mention AI or automation
- Use first name if available
- Sender is starting out and says so (disarming, not weak)""",
    model_settings={"temperature": 0.7}
)

# Lead Enrichment Agent (Step 3.5 Flash)
enrichment_agent = Agent(
    model=DEFAULT_MODEL,
    output_type=LeadEnrichment,
    system_prompt="""Extract enrichment data from research on Australian trades businesses.

Look for: contact name, pain points (slow response, missed calls), 
gaps (no booking widget, dead social), outreach angle.

Output confidence based on data quality.""",
    model_settings={"temperature": 0.2}
)

# Research Agent (Step 3.5 Flash - augments autoresearch.py)
research_agent = Agent(
    model=DEFAULT_MODEL,
    output_type=ResearchOutput,
    system_prompt="""You are a research assistant that analyzes information and produces structured outputs.

Given research data or a topic to investigate:
1. Provide a concise summary (2-3 sentences)
2. Extract 3-5 key findings as bullet points
3. List sources if available
4. Suggest specific actions based on findings
5. Optionally structure data for export (CSV/JSON format ideas)

Be factual, cite specifics, flag uncertainty.""",
    model_settings={"temperature": 0.3}
)

# Feature Implementation Agent (Step 3.5 Flash - self-approving)
class FeaturePlan(BaseModel):
    """Plan for implementing a feature"""
    feature_name: str
    description: str
    files_to_create: List[dict]  # path, description, proposed_content
    files_to_edit: List[dict]   # path, description, old_string, new_string
    commands_to_run: List[dict]  # command, description, cwd
    dependencies: List[str]
    estimated_time: str
    risk_level: Literal["low", "medium", "high"]
    reasoning: str

feature_agent = Agent(
    model=DEFAULT_MODEL,
    output_type=FeaturePlan,
    system_prompt="""You are a software engineer that plans feature implementations.

When given a feature request:
1. Analyze what needs to be built
2. Identify files to create or edit
3. Plan shell commands if needed
4. Assess risk level (low=simple script, high=database changes)
5. Provide reasoning for your approach

You do NOT execute - you only PLAN. Execution requires human approval.""",
    model_settings={"temperature": 0.4}
)


# ============================================================================
# Agent Action Approval System (Self-approval with human checkpoints)
# ============================================================================

class PendingAction(BaseModel):
    """An action requiring human approval before execution"""
    action_id: str
    action_type: Literal["file_write", "file_edit", "shell_command", "code_execution"]
    description: str
    details: dict  # File path, command, etc.
    risk_level: Literal["low", "medium", "high"]
    proposed_at: datetime
    
class ActionResult(BaseModel):
    """Result of an executed action"""
    action_id: str
    success: bool
    output: str
    error: Optional[str] = None
    executed_at: datetime

# Store pending actions (in production, use Redis/SQLite)
_pending_actions: dict[str, PendingAction] = {}
_action_results: dict[str, ActionResult] = {}

def request_approval(action: PendingAction) -> str:
    """Queue an action for human approval, return action_id"""
    _pending_actions[action.action_id] = action
    return action.action_id

def approve_action(action_id: str) -> Optional[ActionResult]:
    """Approve and execute a pending action"""
    if action_id not in _pending_actions:
        return None
    
    action = _pending_actions.pop(action_id)
    
    # Execute based on type
    try:
        if action.action_type == "file_write":
            result = _execute_file_write(action.details)
        elif action.action_type == "file_edit":
            result = _execute_file_edit(action.details)
        elif action.action_type == "shell_command":
            result = _execute_shell(action.details)
        else:
            result = {"success": False, "error": "Unknown action type"}
        
        action_result = ActionResult(
            action_id=action_id,
            success=result.get("success", False),
            output=result.get("output", ""),
            error=result.get("error"),
            executed_at=datetime.now()
        )
        _action_results[action_id] = action_result
        return action_result
        
    except Exception as e:
        error_result = ActionResult(
            action_id=action_id,
            success=False,
            output="",
            error=str(e),
            executed_at=datetime.now()
        )
        _action_results[action_id] = error_result
        return error_result

def _execute_file_write(details: dict) -> dict:
    """Execute a file write operation"""
    path = details.get("path")
    content = details.get("content")
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(content)
    return {"success": True, "output": f"Written {len(content)} chars to {path}"}

def _execute_file_edit(details: dict) -> dict:
    """Execute a file edit operation"""
    path = details.get("path")
    old_string = details.get("old_string")
    new_string = details.get("new_string")
    
    with open(path, 'r') as f:
        content = f.read()
    
    if old_string not in content:
        return {"success": False, "error": "Old string not found in file"}
    
    content = content.replace(old_string, new_string, 1)
    
    with open(path, 'w') as f:
        f.write(content)
    
    return {"success": True, "output": f"Edited {path}"}

def _execute_shell(details: dict) -> dict:
    """Execute a shell command"""
    command = details.get("command")
    cwd = details.get("cwd")
    
    result = subprocess.run(
        command, 
        shell=True, 
        cwd=cwd,
        capture_output=True, 
        text=True,
        timeout=300  # 5 minute timeout
    )
    
    return {
        "success": result.returncode == 0,
        "output": result.stdout,
        "error": result.stderr if result.returncode != 0 else None
    }

def get_pending_actions() -> List[PendingAction]:
    """Get all pending actions requiring approval"""
    return list(_pending_actions.values())

def get_action_result(action_id: str) -> Optional[ActionResult]:
    """Get result of an executed action"""
    return _action_results.get(action_id)

async def classify_reply(email_content: str, lead_context: str = "") -> ReplyClassification:
    """
    Classify an email reply from a lead.
    
    Args:
        email_content: The email body to classify
        lead_context: Optional context about the lead (previous touches, etc.)
    
    Returns:
        ReplyClassification with intent, confidence, next_action
    
    Expected time: 2-5 seconds vs 20+ minutes with MCP
    """
    prompt = f"""Email content:
{email_content}

{lead_context if lead_context else ""}

Classify this reply."""
    
    result = await classifier_agent.run(prompt)
    return result.data


async def generate_draft(
    lead_name: str,
    trade: str,
    city: str,
    detected_gap: str,
    pain_point: Optional[str] = None,
    touch_number: int = 1
) -> DraftGeneration:
    """
    Generate an outreach email draft.
    
    Args:
        lead_name: First name of the contact
        trade: Trade type (plumber, electrician, etc.)
        city: City (Sydney, etc.)
        detected_gap: The specific gap detected for this business
        pain_point: Optional detected pain point
        touch_number: Which touch in sequence (1-6+)
    
    Returns:
        DraftGeneration with subject, body, tone
    """
    prompt = f"""Lead: {lead_name}
Trade: {trade}
City: {city}
Detected gap: {detected_gap}
Pain point: {pain_point or "Not specified"}
Touch number: {touch_number}

Draft an outreach email following the voice and rules."""
    
    result = await draft_agent.run(prompt)
    return result.data


async def enrich_lead(research_data: str) -> LeadEnrichment:
    """
    Extract enrichment data from research.
    
    Args:
        research_data: Scraped research data (website text, reviews, etc.)
    
    Returns:
        LeadEnrichment with contact_name, pain_detected, gap_primary, outreach_angle
    """
    result = await enrichment_agent.run(research_data)
    return result.data


async def conduct_research(query: str, research_data: str) -> ResearchOutput:
    """
    Conduct research on a topic.
    
    Args:
        query: The research query
        research_data: The data to analyze
    
    Returns:
        ResearchOutput with summary, key_findings, sources, suggested_actions
    """
    prompt = f"""Query: {query}

Research data:
{research_data}

Analyze and provide structured output."""
    
    result = await research_agent.run(prompt)
    return result.data


async def plan_feature(feature_request: str, codebase_context: str = "") -> tuple[FeaturePlan, List[str]]:
    """
    Plan a feature implementation with self-approval workflow.
    
    Returns:
        - FeaturePlan with all proposed changes
        - List of action_ids requiring human approval
    """
    prompt = f"""Plan implementation for: {feature_request}

Codebase context:
{codebase_context}

Provide detailed implementation plan with files to create/edit and commands to run.
All actions will require human approval before execution."""

    result = await feature_agent.run(prompt)
    plan = result.data
    
    # Queue actions for approval
    action_ids = []
    
    for file_op in plan.files_to_create:
        action = PendingAction(
            action_id=f"file_create_{datetime.now().timestamp()}_{len(action_ids)}",
            action_type="file_write",
            description=f"Create file: {file_op.get('path')}",
            details=file_op,
            risk_level=plan.risk_level,
            proposed_at=datetime.now()
        )
        action_ids.append(request_approval(action))
    
    for file_op in plan.files_to_edit:
        action = PendingAction(
            action_id=f"file_edit_{datetime.now().timestamp()}_{len(action_ids)}",
            action_type="file_edit",
            description=f"Edit file: {file_op.get('path')}",
            details=file_op,
            risk_level=plan.risk_level,
            proposed_at=datetime.now()
        )
        action_ids.append(request_approval(action))
    
    for cmd in plan.commands_to_run:
        action = PendingAction(
            action_id=f"shell_{datetime.now().timestamp()}_{len(action_ids)}",
            action_type="shell_command",
            description=f"Run: {cmd.get('command')}",
            details=cmd,
            risk_level="high" if "rm" in cmd.get('command', '') or "drop" in cmd.get('command', '') else plan.risk_level,
            proposed_at=datetime.now()
        )
        action_ids.append(request_approval(action))
    
    return plan, action_ids


def format_client_report(report: ClientReport, platform: str) -> str:
    """
    Format a client report for a chat platform.
    
    Args:
        report: The client report
        platform: The chat platform (whatsapp, telegram, etc.)
    
    Returns:
        The formatted report
    """
    if platform == "whatsapp":
        return f"""Daily Report ({report.period})
Replies: {report.replies_received} received, {report.replies_handled} handled
Conversions: {report.conversions}
Pipeline: {report.pipeline_value}
Urgent: {", ".join(report.urgent_items)}
Summary: {report.daily_summary}"""
    elif platform == "telegram":
        return f"""Daily Report ({report.period})

Replies:
• Received: {report.replies_received}
• Handled: {report.replies_handled}

Conversions: {report.conversions}
Pipeline: {report.pipeline_value}

Urgent Items:
{", ".join(report.urgent_items)}

Summary: {report.daily_summary}"""
    else:
        raise ValueError("Unsupported platform")


# ============================================================================
# Synchronous Wrappers (for easier integration)
# ============================================================================

def classify_reply_sync(email_content: str, lead_context: str = "") -> ReplyClassification:
    """Synchronous wrapper for classify_reply"""
    return asyncio.run(classify_reply(email_content, lead_context))


def generate_draft_sync(
    lead_name: str,
    trade: str,
    city: str,
    detected_gap: str,
    pain_point: Optional[str] = None,
    touch_number: int = 1
) -> DraftGeneration:
    """Synchronous wrapper for generate_draft"""
    return asyncio.run(generate_draft(lead_name, trade, city, detected_gap, pain_point, touch_number))


def enrich_lead_sync(research_data: str) -> LeadEnrichment:
    """Synchronous wrapper for enrich_lead"""
    return asyncio.run(enrich_lead(research_data))


def conduct_research_sync(query: str, research_data: str) -> ResearchOutput:
    """Synchronous wrapper for conduct_research"""
    return asyncio.run(conduct_research(query, research_data))


def plan_feature_sync(feature_request: str, codebase_context: str = "") -> tuple[FeaturePlan, List[str]]:
    """Synchronous wrapper for plan_feature with self-approval"""
    return asyncio.run(plan_feature(feature_request, codebase_context))


# ============================================================================
# WhatsApp + Telegram Channel Integration (Simple Chat-Based UX)
# ============================================================================

from typing import Protocol
import aiohttp

class ChatChannel(Protocol):
    """Protocol for chat channels (WhatsApp, Telegram)"""
    async def send_message(self, recipient: str, message: str) -> bool:
        """Send a text message"""
        ...
    
    async def send_report(self, recipient: str, report: ClientReport) -> bool:
        """Send a formatted report"""
        ...
    
    def format_message(self, text: str) -> str:
        """Format message for this platform"""
        ...


class TelegramChannel:
    """Telegram Bot API integration"""
    
    def __init__(self, bot_token: str):
        self.bot_token = bot_token
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
    
    async def send_message(self, chat_id: str, message: str) -> bool:
        """Send message via Telegram Bot API"""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                return resp.status == 200
    
    async def send_report(self, chat_id: str, report: ClientReport) -> bool:
        """Send formatted report to Telegram"""
        message = format_client_report(report, "telegram")
        return await self.send_message(chat_id, message)
    
    def format_message(self, text: str) -> str:
        """Format for Telegram (HTML tags allowed)"""
        # Telegram supports HTML: <b>, <i>, <code>, etc.
        return text


class WhatsAppChannel:
    """WhatsApp Business API integration (via Twilio or Meta Direct)"""
    
    def __init__(self, account_sid: str, auth_token: str, from_number: str, provider: str = "twilio"):
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.from_number = from_number
        self.provider = provider
        
        if provider == "twilio":
            self.base_url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
        else:
            # Meta Business API direct
            self.base_url = "https://graph.facebook.com/v18.0/me/messages"
    
    async def send_message(self, to_number: str, message: str) -> bool:
        """Send WhatsApp message"""
        if self.provider == "twilio":
            return await self._send_twilio(to_number, message)
        else:
            return await self._send_meta(to_number, message)
    
    async def _send_twilio(self, to_number: str, message: str) -> bool:
        """Send via Twilio WhatsApp API"""
        import aiohttp
        
        payload = {
            "From": f"whatsapp:{self.from_number}",
            "To": f"whatsapp:{to_number}",
            "Body": message
        }
        
        auth = aiohttp.BasicAuth(self.account_sid, self.auth_token)
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.base_url, data=payload, auth=auth) as resp:
                return resp.status == 201
    
    async def _send_meta(self, to_number: str, message: str) -> bool:
        """Send via Meta Business API (requires template outside 24h window)"""
        # For proactive messages, Meta requires approved templates
        # This is simplified - real implementation needs template management
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "text",
            "text": {"body": message}
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.base_url, json=payload, headers=headers) as resp:
                return resp.status == 200
    
    async def send_template_message(self, to_number: str, template_name: str, language_code: str = "en", params: list = None) -> bool:
        """Send approved template message (required for proactive WhatsApp messages)"""
        headers = {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code},
                "components": params or []
            }
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(self.base_url, json=payload, headers=headers) as resp:
                return resp.status == 200
    
    async def send_report(self, to_number: str, report: ClientReport) -> bool:
        """Send formatted report to WhatsApp"""
        message = format_client_report(report, "whatsapp")
        return await self.send_message(to_number, message)
    
    def format_message(self, text: str) -> str:
        """Format for WhatsApp (markdown-style)"""
        # WhatsApp supports: *bold*, _italic_, ~strikethrough~, ```code```
        return text


class ChatRouter:
    """Route notifications to client's preferred channel"""
    
    def __init__(self):
        self.channels: dict[str, ChatChannel] = {}
    
    def register_channel(self, client_id: str, channel: ChatChannel, channel_type: str):
        """Register a channel for a client"""
        self.channels[f"{client_id}:{channel_type}"] = channel
    
    async def notify(self, client_id: str, message: str, priority: str = "normal", preferred_channel: str = None):
        """Send notification to client's preferred channel"""
        
        # If client has preferred channel, use it
        if preferred_channel:
            channel_key = f"{client_id}:{preferred_channel}"
            if channel_key in self.channels:
                return await self.channels[channel_key].send_message(
                    self._get_recipient(client_id, preferred_channel), 
                    message
                )
        
        # Fallback: try all registered channels
        for channel_type in ["telegram", "whatsapp"]:
            channel_key = f"{client_id}:{channel_type}"
            if channel_key in self.channels:
                return await self.channels[channel_key].send_message(
                    self._get_recipient(client_id, channel_type),
                    message
                )
        
        return False
    
    async def send_report(self, client_id: str, report: ClientReport, channel_type: str = "telegram"):
        """Send daily/weekly report to client"""
        channel_key = f"{client_id}:{channel_type}"
        if channel_key in self.channels:
            return await self.channels[channel_key].send_report(
                self._get_recipient(client_id, channel_type),
                report
            )
        return False
    
    def _get_recipient(self, client_id: str, channel_type: str) -> str:
        """Get recipient ID (chat_id or phone number) for client"""
        # In production, lookup from database
        # For now, assume client_id maps directly or look up
        return client_id  # Simplified


# Quick setup helper
def create_telegram_channel(bot_token: str = None) -> TelegramChannel:
    """Create Telegram channel from env or provided token"""
    token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("Telegram bot token required")
    return TelegramChannel(token)


def create_whatsapp_channel(
    account_sid: str = None,
    auth_token: str = None,
    from_number: str = None,
    provider: str = "twilio"
) -> WhatsAppChannel:
    """Create WhatsApp channel from env or provided credentials"""
    sid = account_sid or os.getenv("TWILIO_ACCOUNT_SID") or os.getenv("WHATSAPP_ACCOUNT_SID")
    token = auth_token or os.getenv("TWILIO_AUTH_TOKEN") or os.getenv("WHATSAPP_AUTH_TOKEN")
    number = from_number or os.getenv("WHATSAPP_FROM_NUMBER")
    
    if not all([sid, token, number]):
        raise ValueError("WhatsApp credentials required (account_sid, auth_token, from_number)")
    
    return WhatsAppChannel(sid, token, number, provider)


# ============================================================================
# Example Usage / Testing
# ============================================================================

if __name__ == "__main__":
    async def demo():
        print("=" * 60)
        print("Pydantic AI Agent with Step 3.5 Flash")
        print("Features: Classification | Drafts | Research | Self-Approval")
        print("=" * 60)
        
        # Demo 1: Reply Classification
        print("\n1. Reply Classification:")
        test_reply = """Hi Yegor,

Thanks for reaching out. I'm actually looking for someone to help with our enquiry handling — we miss too many calls while on jobs. Can you tell me more about what you do?

Cheers,
Mike"""
        
        result = await classify_reply(test_reply)
        print(f"   Intent: {result.intent}")
        print(f"   Confidence: {result.confidence:.2f}")
        print(f"   Next Action: {result.next_action}")
        
        # Demo 2: Draft Generation
        print("\n2. Draft Generation:")
        draft = await generate_draft(
            lead_name="Mike",
            trade="plumber",
            city="Sydney",
            detected_gap="no online booking widget on website",
            pain_point="missed calls while on jobs",
            touch_number=1
        )
        print(f"   Subject: {draft.subject}")
        print(f"   Body:\n{draft.body}")
        
        # Demo 3: Research (augments autoresearch)
        print("\n3. Research Analysis:")
        research_data = """
        Website: mikesplumbing.com.au
        Reviews: 45 Google reviews, 4.2 stars
        Recent reviews mention: "never called back", "great work but hard to reach"
        Social: Last Facebook post 4 months ago
        Website: No booking form, only phone number
        """
        research = await conduct_research("Analyze lead potential for Mike's Plumbing", research_data)
        print(f"   Summary: {research.summary}")
        print(f"   Key Findings: {research.key_findings}")
        print(f"   Suggested Actions: {research.suggested_actions}")
        
        # Demo 4: Client Report for Chat
        print("\n4. Client Report (Chat Format):")
        report = ClientReport(
            period="Daily",
            replies_received=5,
            replies_handled=3,
            conversions=1,
            pipeline_value="$3,200",
            urgent_items=["Mike (Plumber) - wants a call", "Sarah - requested quote"],
            daily_summary="Good day - 3 replies handled, 1 conversion from a $2,400 job."
        )
        
        print("\n   WhatsApp format:")
        print(format_client_report(report, "whatsapp"))
        
        print("\n   Telegram format:")
        print(format_client_report(report, "telegram"))
        
        # Demo 5: Feature Planning with Self-Approval
        print("\n5. Feature Planning (Self-Approval System):")
        print("   Requesting feature plan...")
        
        plan, action_ids = await plan_feature(
            "Add daily report scheduler that sends client reports every morning",
            "Current codebase uses SQLite, has telegram bot, uses pydantic-ai agent"
        )
        
        print(f"   Feature: {plan.feature_name}")
        print(f"   Risk Level: {plan.risk_level}")
        print(f"   Files to create: {len(plan.files_to_create)}")
        print(f"   Files to edit: {len(plan.files_to_edit)}")
        print(f"   Pending approvals: {len(action_ids)}")
        print(f"   Action IDs: {action_ids}")
        
        print("\n   Pending actions for approval:")
        for action in get_pending_actions():
            print(f"   - [{action.action_type}] {action.description} ({action.risk_level} risk)")
        
        print("\n" + "=" * 60)
        print("Demo complete! Self-approval system ready.")
        print("Use approve_action(action_id) to execute pending changes.")
        print("=" * 60)
    
    # Run demo
    asyncio.run(demo())
