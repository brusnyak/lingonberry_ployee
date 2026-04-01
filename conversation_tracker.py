"""
Conversation state tracking
Manages lead conversation lifecycle: new → contacted → replied → negotiating → closed_won/closed_lost
"""
import sqlite3
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, List
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "leadgen" / "data" / "leads.db"


class ConversationState(str, Enum):
    """Conversation lifecycle states"""
    NEW = "new"
    CONTACTED = "contacted"           # Initial outreach sent
    REPLIED = "replied"               # Lead responded
    NEGOTIATING = "negotiating"       # Active back-and-forth
    CLOSED_WON = "closed_won"         # Deal won
    CLOSED_LOST = "closed_lost"       # Deal lost or no response
    FOLLOWUP = "followup"             # Scheduled for follow-up


class ConversationTracker:
    """Tracks conversation state transitions and history"""
    
    def __init__(self):
        self.conn = None
    
    def _get_db(self) -> sqlite3.Connection:
        """Get database connection"""
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    
    def create_or_update_conversation(self, 
                                       lead_id: int,
                                       client_id: Optional[str] = None,
                                       state: ConversationState = ConversationState.NEW,
                                       outreach_id: Optional[int] = None,
                                       reply_id: Optional[int] = None,
                                       notes: Optional[str] = None,
                                       pipeline_value: Optional[str] = None,
                                       next_action: Optional[str] = None) -> int:
        """
        Create a new conversation or update existing one.
        Returns conversation ID.
        """
        conn = self._get_db()
        try:
            cursor = conn.cursor()
            now = datetime.now(timezone.utc).isoformat()
            
            # Check if conversation exists for this lead
            cursor.execute(
                "SELECT id FROM conversations WHERE lead_id = ? AND client_id = ? AND state NOT IN ('closed_won', 'closed_lost')",
                (lead_id, client_id or 'operator')
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing conversation
                conv_id = existing['id']
                
                # Update appropriate fields based on state change
                update_fields = ["state = ?, updated_at = ?, last_activity_at = ?"]
                params = [state.value, now, now]
                
                if outreach_id:
                    update_fields.append("last_outreach_id = ?")
                    params.append(outreach_id)
                
                if reply_id:
                    update_fields.append("last_reply_id = ?")
                    params.append(reply_id)
                    update_fields.append("last_reply_at = ?")
                    params.append(now)
                
                if notes:
                    # Append to existing notes
                    cursor.execute("SELECT notes FROM conversations WHERE id = ?", (conv_id,))
                    existing_notes = cursor.fetchone()['notes'] or ""
                    new_notes = f"{existing_notes}\n[{now}] {notes}".strip()
                    update_fields.append("notes = ?")
                    params.append(new_notes)
                
                if pipeline_value:
                    update_fields.append("pipeline_value = ?")
                    params.append(pipeline_value)
                
                if next_action:
                    update_fields.append("next_action = ?")
                    params.append(next_action)
                
                params.append(conv_id)
                
                query = f"UPDATE conversations SET {', '.join(update_fields)} WHERE id = ?"
                cursor.execute(query, params)
                
            else:
                # Create new conversation
                cursor.execute("""
                    INSERT INTO conversations (
                        client_id, lead_id, state, 
                        created_at, updated_at, last_activity_at,
                        notes, pipeline_value, next_action
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    client_id or 'operator',
                    lead_id,
                    state.value,
                    now, now, now,
                    notes or "",
                    pipeline_value,
                    next_action
                ))
                conv_id = cursor.lastrowid
            
            # Log state transition
            if reply_id:
                cursor.execute("""
                    INSERT INTO conversation_events (
                        conversation_id, event_type, event_data, created_at
                    ) VALUES (?, 'reply_received', ?, ?)
                """, (conv_id, f"reply_id: {reply_id}", now))
            
            conn.commit()
            return conv_id
            
        finally:
            conn.close()
    
    def transition_state(self, conversation_id: int, 
                        new_state: ConversationState,
                        notes: Optional[str] = None) -> bool:
        """Transition conversation to new state"""
        conn = self._get_db()
        try:
            cursor = conn.cursor()
            now = datetime.now(timezone.utc).isoformat()
            
            # Get current state for logging
            cursor.execute("SELECT state FROM conversations WHERE id = ?", (conversation_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            old_state = row['state']
            
            # Update state
            cursor.execute("""
                UPDATE conversations 
                SET state = ?, updated_at = ?, last_activity_at = ?
                WHERE id = ?
            """, (new_state.value, now, now, conversation_id))
            
            # Log transition
            event_data = f"{old_state} → {new_state.value}"
            if notes:
                event_data += f" | {notes}"
            
            cursor.execute("""
                INSERT INTO conversation_events (
                    conversation_id, event_type, event_data, created_at
                ) VALUES (?, 'state_transition', ?, ?)
            """, (conversation_id, event_data, now))
            
            conn.commit()
            return True
            
        finally:
            conn.close()
    
    def get_conversation(self, conversation_id: int) -> Optional[Dict]:
        """Get conversation details"""
        conn = self._get_db()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT c.*, b.name as lead_name, b.email_maps as lead_email, b.phone
                FROM conversations c
                JOIN businesses b ON b.id = c.lead_id
                WHERE c.id = ?
            """, (conversation_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    
    def get_conversations_by_client(self, client_id: str, 
                                     state: Optional[ConversationState] = None,
                                     limit: int = 50) -> List[Dict]:
        """Get all conversations for a client"""
        conn = self._get_db()
        try:
            cursor = conn.cursor()
            
            if state:
                cursor.execute("""
                    SELECT c.*, b.name as lead_name, b.email_maps, b.phone, b.category
                    FROM conversations c
                    JOIN businesses b ON b.id = c.lead_id
                    WHERE c.client_id = ? AND c.state = ?
                    ORDER BY c.last_activity_at DESC
                    LIMIT ?
                """, (client_id, state.value, limit))
            else:
                cursor.execute("""
                    SELECT c.*, b.name as lead_name, b.email_maps, b.phone, b.category
                    FROM conversations c
                    JOIN businesses b ON b.id = c.lead_id
                    WHERE c.client_id = ?
                    ORDER BY c.last_activity_at DESC
                    LIMIT ?
                """, (client_id, limit))
            
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()
    
    def get_pipeline_stats(self, client_id: Optional[str] = None) -> Dict:
        """Get pipeline statistics"""
        conn = self._get_db()
        try:
            cursor = conn.cursor()
            
            if client_id:
                cursor.execute("""
                    SELECT state, COUNT(*) as count
                    FROM conversations
                    WHERE client_id = ?
                    GROUP BY state
                """, (client_id,))
            else:
                cursor.execute("""
                    SELECT state, COUNT(*) as count
                    FROM conversations
                    GROUP BY state
                """)
            
            stats = {row['state']: row['count'] for row in cursor.fetchall()}
            
            # Calculate totals
            active = sum(stats.get(s.value, 0) for s in 
                        [ConversationState.NEW, ConversationState.CONTACTED, 
                         ConversationState.REPLIED, ConversationState.NEGOTIATING])
            closed_won = stats.get(ConversationState.CLOSED_WON.value, 0)
            closed_lost = stats.get(ConversationState.CLOSED_LOST.value, 0)
            
            return {
                'by_state': stats,
                'active': active,
                'closed_won': closed_won,
                'closed_lost': closed_lost,
                'total': active + closed_won + closed_lost
            }
        finally:
            conn.close()
    
    def add_event(self, conversation_id: int, event_type: str, event_data: str) -> bool:
        """Add an event to conversation history"""
        conn = self._get_db()
        try:
            cursor = conn.cursor()
            now = datetime.now(timezone.utc).isoformat()
            
            cursor.execute("""
                INSERT INTO conversation_events (
                    conversation_id, event_type, event_data, created_at
                ) VALUES (?, ?, ?, ?)
            """, (conversation_id, event_type, event_data, now))
            
            # Also update last_activity_at
            cursor.execute("""
                UPDATE conversations 
                SET last_activity_at = ?
                WHERE id = ?
            """, (now, conversation_id))
            
            conn.commit()
            return True
        finally:
            conn.close()


# SQL for creating conversation tables (run once)
CONVERSATION_TABLES_SQL = """
-- Conversations table (already created in migration, this is for reference)
CREATE TABLE IF NOT EXISTS conversations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id TEXT NOT NULL,
    lead_id INTEGER NOT NULL,
    state TEXT DEFAULT 'new',
    last_outreach_id INTEGER,
    last_reply_id INTEGER,
    last_outreach_at TEXT,
    last_reply_at TEXT,
    last_activity_at TEXT,
    notes TEXT,
    pipeline_value TEXT,
    next_action TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (client_id) REFERENCES clients(id),
    FOREIGN KEY (lead_id) REFERENCES businesses(id)
);

-- Conversation events log
CREATE TABLE IF NOT EXISTS conversation_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_data TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_conversations_client_id ON conversations(client_id);
CREATE INDEX IF NOT EXISTS idx_conversations_lead_id ON conversations(lead_id);
CREATE INDEX IF NOT EXISTS idx_conversations_state ON conversations(state);
CREATE INDEX IF NOT EXISTS idx_conversation_events_conv_id ON conversation_events(conversation_id);
"""


def init_conversation_tables():
    """Initialize conversation tracking tables"""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(CONVERSATION_TABLES_SQL)
        conn.commit()
        print("✅ Conversation tables initialized")
    except Exception as e:
        print(f"❌ Error initializing tables: {e}")
    finally:
        conn.close()


# Convenience functions

def track_outreach_sent(lead_id: int, client_id: Optional[str] = None, 
                        outreach_id: Optional[int] = None) -> int:
    """Record that outreach was sent to a lead"""
    tracker = ConversationTracker()
    return tracker.create_or_update_conversation(
        lead_id=lead_id,
        client_id=client_id,
        state=ConversationState.CONTACTED,
        outreach_id=outreach_id,
        notes="Initial outreach sent"
    )


def track_reply_received(lead_id: int, client_id: Optional[str] = None,
                        reply_id: Optional[int] = None) -> int:
    """Record that a lead replied"""
    tracker = ConversationTracker()
    return tracker.create_or_update_conversation(
        lead_id=lead_id,
        client_id=client_id,
        state=ConversationState.REPLIED,
        reply_id=reply_id,
        notes="Reply received"
    )


def close_conversation(conversation_id: int, won: bool = True, 
                       notes: Optional[str] = None) -> bool:
    """Close a conversation as won or lost"""
    tracker = ConversationTracker()
    new_state = ConversationState.CLOSED_WON if won else ConversationState.CLOSED_LOST
    return tracker.transition_state(conversation_id, new_state, notes)


if __name__ == "__main__":
    init_conversation_tables()
    
    # Test
    tracker = ConversationTracker()
    
    # Test creating a conversation
    conv_id = tracker.create_or_update_conversation(
        lead_id=1,
        client_id='test_client',
        state=ConversationState.CONTACTED,
        notes="Test outreach"
    )
    print(f"Created conversation: {conv_id}")
    
    # Test transition
    tracker.transition_state(conv_id, ConversationState.REPLIED, "Lead responded")
    
    # Get stats
    stats = tracker.get_pipeline_stats('test_client')
    print(f"Stats: {stats}")
