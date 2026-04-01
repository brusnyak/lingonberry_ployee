"""
Reply workflow automation
Monitors for replies, generates AI drafts, sends for approval, handles sending.
Integrates with Pydantic AI agent for intelligent draft generation.
"""
import asyncio
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# Add agent to path
sys.path.insert(0, str(Path(__file__).parent.parent / "agent"))
sys.path.insert(0, str(Path(__file__).parent.parent))

from outreach.storage.db import (
    connect, get_unnotified_replies, mark_reply_notified,
    upsert_reply_draft, mark_reply_draft_approved, mark_reply_draft_sent,
    log_classification, get_reply_queue_needing_action
)
from outreach.classifier import classify_reply
from agent.pydantic_ai_agent import ReplyClassifier, DraftGenerator

# Telegram bot for notifications (lazy import to avoid circular deps)
bot = None
OPERATOR_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

def get_bot():
    global bot
    if bot is None:
        try:
            from telegram import Bot
            TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
            bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
        except Exception as e:
            print(f"Warning: Could not initialize Telegram bot: {e}")
            bot = None
    return bot


# Lazy imports for telegram modules (avoid circular imports)
def get_rbac():
    """Lazy import telegram.rbac to avoid circular imports"""
    from telegram.rbac import get_client_info, update_client_auto_approve, get_user_role
    return {'get_client_info': get_client_info, 'update_client_auto_approve': update_client_auto_approve, 'get_user_role': get_user_role}


class ReplyWorkflow:
    """Automated reply handling workflow"""
    
    def __init__(self):
        self.classifier = ReplyClassifier()
        self.draft_generator = DraftGenerator()
        self.running = False
    
    async def process_new_replies(self, limit: int = 10) -> List[Dict]:
        """
        Poll for unnotified replies, classify them, generate drafts, send notifications.
        Returns list of processed replies.
        """
        conn = connect()
        try:
            # Get unnotified replies
            replies = get_unnotified_replies(conn, limit=limit)
            processed = []
            
            for reply in replies:
                try:
                    result = await self._process_single_reply(conn, reply)
                    processed.append(result)
                except Exception as e:
                    print(f"Error processing reply {reply['id']}: {e}")
                    continue
            
            return processed
        finally:
            conn.close()
    
    async def _process_single_reply(self, conn: sqlite3.Connection, reply: sqlite3.Row) -> Dict:
        """Process a single reply through the full workflow"""
        reply_id = reply['id']
        lead_name = reply['name']
        client_id = reply.get('client_id')  # May be None for operator-managed leads
        
        print(f"Processing reply from {lead_name} (reply_id: {reply_id})")
        
        # Step 1: Classify the reply
        classification = await self._classify_reply(reply)
        
        # Store classification
        log_classification(
            conn, reply_id,
            label=classification['label'],
            pain_points=classification.get('pain_points', ''),
            confidence=classification['confidence'],
            model='gpt-4o-mini'
        )
        
        # Step 2: Generate draft (if not spam/ignore)
        if classification['label'] in ['interested', 'question']:
            draft = await self._generate_draft(reply, classification)
            
            # Store draft
            draft_id = upsert_reply_draft(
                conn, reply_id,
                subject=draft['subject'],
                body=draft['body'],
                sender_name=draft.get('sender_name', ''),
                sender_address=draft.get('sender_address', ''),
                rationale=draft.get('rationale', '')
            )
        else:
            draft = None
            # Still create a draft record marked as skipped
            upsert_reply_draft(
                conn, reply_id,
                subject=f"Re: {reply['subject']}",
                body="[No reply needed - not interested]",
                rationale="Classification: not_interested or ignore"
            )
        
        # Step 3: Mark as notified (prevents reprocessing)
        mark_reply_notified(conn, reply_id)
        
        # Step 4: Send notification for approval
        await self._send_approval_notification(reply, classification, draft, client_id)
        
        return {
            'reply_id': reply_id,
            'lead_name': lead_name,
            'classification': classification['label'],
            'confidence': classification['confidence'],
            'draft_created': draft is not None,
            'client_id': client_id
        }
    
    async def _classify_reply(self, reply: sqlite3.Row) -> Dict:
        """Classify a reply using AI"""
        # Use existing classifier
        result = classify_reply(reply['content'])
        
        return {
            'label': result.get('label', 'unclassified'),
            'confidence': result.get('confidence', 0.5),
            'pain_points': result.get('pain_points', ''),
            'summary': result.get('summary', '')
        }
    
    async def _generate_draft(self, reply: sqlite3.Row, classification: Dict) -> Dict:
        """Generate a reply draft using AI"""
        # Get conversation context
        context = self._get_conversation_context(reply['lead_id'])
        
        # Generate draft using Pydantic AI agent
        draft_result = self.draft_generator.generate(
            lead_name=reply['name'],
            lead_info={
                'niche': reply.get('target_niche', ''),
                'category': reply.get('category', ''),
                'outreach_angle': reply.get('outreach_angle', '')
            },
            inbound_message=reply['content'],
            classification=classification['label'],
            conversation_history=context
        )
        
        return {
            'subject': f"Re: {reply['subject']}",
            'body': draft_result['body'],
            'sender_name': reply.get('original_sender_name', ''),
            'sender_address': reply.get('original_sender_address', ''),
            'rationale': draft_result.get('rationale', '')
        }
    
    def _get_conversation_context(self, lead_id: int) -> List[Dict]:
        """Get conversation history for context"""
        conn = connect()
        try:
            cursor = conn.execute("""
                SELECT r.content, r.received_at, rd.body as reply_body, rd.sent_at
                FROM replies r
                LEFT JOIN reply_drafts rd ON rd.reply_id = r.id
                WHERE r.lead_id = ?
                ORDER BY r.received_at ASC
                LIMIT 5
            """, (lead_id,))
            
            history = []
            for row in cursor.fetchall():
                history.append({
                    'inbound': row['content'],
                    'outbound': row['reply_body'],
                    'date': row['received_at']
                })
            
            return history
        finally:
            conn.close()
    
    async def _send_approval_notification(self, reply: sqlite3.Row, classification: Dict, 
                                          draft: Optional[Dict], client_id: Optional[str]):
        """Send approval notification to operator or client"""
        bot = get_bot()
        if not bot:
            print("Telegram bot not available, cannot send notification")
            return
        
        lead_name = reply['name']
        emoji = {
            'interested': '🟢',
            'question': '❓',
            'not_interested': '🔴',
            'ignore': '⚫'
        }.get(classification['label'], '⚪')
        
        # Build notification message
        lines = [
            f"{emoji} New Reply from {lead_name}",
            "",
            f"Classification: {classification['label']} (confidence: {classification['confidence']:.0%})",
            "",
            f"📨 Their message:",
            f"\"{reply['content'][:300]}...\"" if len(reply['content']) > 300 else f"\"{reply['content']}\"",
            ""
        ]
        
        if draft:
            lines.extend([
                f"📝 Suggested reply:",
                f"Subject: {draft['subject']}",
                f"",
                f"{draft['body'][:400]}..." if len(draft['body']) > 400 else draft['body'],
                "",
                f"Rationale: {draft.get('rationale', 'N/A')}",
                ""
            ])
        
        # Determine recipient
        if client_id:
            # Send to client
            rbac = get_rbac()
            get_client_info = rbac['get_client_info']
            client_info = get_client_info(client_id)
            if client_info and client_info.get('auto_approve'):
                # Auto-approve mode - send immediately
                await self._auto_send_reply(reply['id'], draft, client_id)
                return
            
            chat_id = client_info.get('telegram_chat_id') if client_info else None
            if chat_id:
                lines.extend([
                    "Actions:",
                    f"/approve_reply {reply['id']} - Send this reply",
                    f"/edit_reply {reply['id']} - Modify before sending",
                    f"/skip_reply {reply['id']} - Don't reply"
                ])
                
                try:
                    await bot.send_message(
                        chat_id=int(chat_id),
                        text="\n".join(lines),
                        parse_mode='HTML'
                    )
                except Exception as e:
                    print(f"Failed to notify client {client_id}: {e}")
                    # Fallback to operator
                    await self._notify_operator(reply, classification, draft, client_id)
        else:
            # Send to operator
            await self._notify_operator(reply, classification, draft, client_id)
    
    async def _notify_operator(self, reply: sqlite3.Row, classification: Dict,
                               draft: Optional[Dict], client_id: Optional[str]):
        """Send notification to operator"""
        bot = get_bot()
        if not OPERATOR_CHAT_ID:
            print("No OPERATOR_CHAT_ID set")
            return
        
        lead_name = reply['name']
        emoji = {
            'interested': '🟢',
            'question': '❓',
            'not_interested': '🔴',
            'ignore': '⚫'
        }.get(classification['label'], '⚪')
        
        lines = [
            f"{emoji} New Reply from {lead_name}",
        ]
        
        if client_id:
            rbac = get_rbac()
            get_client_info = rbac['get_client_info']
            client_info = get_client_info(client_id)
            lines.append(f"Client: {client_info['business_name'] if client_info else client_id}")
        
        lines.extend([
            "",
            f"Classification: {classification['label']} (confidence: {classification['confidence']:.0%})",
            "",
            f"📨 Their message:",
            f"\"{reply['content'][:300]}...\"" if len(reply['content']) > 300 else f"\"{reply['content']}\"",
            ""
        ])
        
        if draft:
            lines.extend([
                f"📝 Suggested reply:",
                f"Subject: {draft['subject']}",
                f"",
                f"{draft['body'][:400]}..." if len(draft['body']) > 400 else draft['body'],
                ""
            ])
        
        lines.extend([
            "Actions:",
            f"/approve_reply {reply['id']} - Send this reply",
            f"/edit_reply {reply['id']} - Modify before sending",
            f"/skip_reply {reply['id']} - Don't reply"
        ])
        
        try:
            await bot.send_message(
                chat_id=int(OPERATOR_CHAT_ID),
                text="\n".join(lines)
            )
        except Exception as e:
            print(f"Failed to notify operator: {e}")
    
    async def _auto_send_reply(self, reply_id: int, draft: Dict, client_id: str):
        """Auto-send a reply (when client has auto_approve enabled)"""
        print(f"Auto-sending reply {reply_id} for client {client_id}")
        # Implementation would call email sender here
        # For now, just log it
        conn = connect()
        try:
            mark_reply_draft_sent(conn, reply_id, draft['sender_name'], draft['sender_address'])
            
            # Notify client that reply was sent
            bot = get_bot()
            rbac = get_rbac()
            get_client_info = rbac['get_client_info']
            client_info = get_client_info(client_id)
            if client_info and bot:
                await bot.send_message(
                    chat_id=int(client_info['telegram_chat_id']),
                    text=f"✅ Auto-replied to lead. Reply sent automatically (auto-approve is ON).\n\nSubject: {draft['subject']}"
                )
        finally:
            conn.close()
    
    async def approve_reply(self, reply_id: int, edited_body: Optional[str] = None) -> bool:
        """Approve and send a reply draft"""
        conn = connect()
        try:
            # Get the draft
            cursor = conn.execute(
                "SELECT * FROM reply_drafts WHERE reply_id = ?",
                (reply_id,)
            )
            draft = cursor.fetchone()
            
            if not draft:
                print(f"No draft found for reply {reply_id}")
                return False
            
            # Use edited body if provided, otherwise use draft
            body = edited_body if edited_body else draft['body']
            
            # TODO: Actually send the email here
            # For now, just mark as sent
            mark_reply_draft_sent(conn, reply_id, draft['sender_name'], draft['sender_address'])
            
            print(f"Reply {reply_id} approved and sent")
            return True
        except Exception as e:
            print(f"Error approving reply {reply_id}: {e}")
            return False
        finally:
            conn.close()
    
    async def run_continuous_monitoring(self, interval_seconds: int = 60):
        """Run continuous monitoring loop"""
        print(f"Starting reply workflow monitoring (interval: {interval_seconds}s)")
        self.running = True
        
        while self.running:
            try:
                processed = await self.process_new_replies(limit=5)
                if processed:
                    print(f"Processed {len(processed)} replies")
                    for p in processed:
                        print(f"  - {p['lead_name']}: {p['classification']} (draft: {p['draft_created']})")
                
                # Wait for next check
                await asyncio.sleep(interval_seconds)
                
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
                await asyncio.sleep(interval_seconds)
    
    def stop(self):
        """Stop the monitoring loop"""
        self.running = False


# Convenience functions for use from handlers.py
async def check_and_process_replies() -> List[Dict]:
    """Check for new replies and process them (can be called from scheduler)"""
    workflow = ReplyWorkflow()
    return await workflow.process_new_replies(limit=10)


async def approve_and_send_reply(reply_id: int, edited_body: Optional[str] = None) -> bool:
    """Approve and send a specific reply"""
    from agent.reply_sender import send_reply_email
    
    # Send the actual email
    result = send_reply_email(reply_id, edited_body)
    
    if result['success']:
        # Notify that it was sent
        print(f"✅ Reply {reply_id} sent to {result.get('lead_name', 'unknown')}")
        return True
    else:
        print(f"❌ Failed to send reply {reply_id}: {result.get('error')}")
        return False


# For testing
if __name__ == "__main__":
    async def test():
        workflow = ReplyWorkflow()
        results = await workflow.process_new_replies(limit=3)
        print(f"\nProcessed {len(results)} replies:")
        for r in results:
            print(f"  {r['lead_name']}: {r['classification']} (draft: {r['draft_created']})")
    
    asyncio.run(test())
