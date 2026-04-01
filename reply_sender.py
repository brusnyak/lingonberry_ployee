"""
Reply email sender integration
Sends approved reply drafts via SMTP using the same accounts as outreach.
"""
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Dict

sys.path.insert(0, str(Path(__file__).parent.parent / "outreach"))
sys.path.insert(0, str(Path(__file__).parent.parent))

from outreach.email_sender import send_email, _load_accounts
from outreach.storage.db import connect


def send_reply_email(reply_id: int, edited_body: Optional[str] = None) -> Dict:
    """
    Send a reply email for an approved draft.
    
    Args:
        reply_id: The reply ID from the replies table
        edited_body: Optional edited body text (if user modified the draft)
        
    Returns:
        Dict with success status and details
    """
    conn = connect()
    try:
        # Get the reply and draft details
        cursor = conn.execute("""
            SELECT 
                r.id as reply_id,
                r.lead_id,
                r.from_address as lead_email,
                r.subject as original_subject,
                rd.id as draft_id,
                rd.subject as draft_subject,
                rd.body as draft_body,
                rd.sender_name,
                rd.sender_address,
                b.name as lead_name,
                ol.sender_name as original_sender_name,
                ol.sender_address as original_sender_address
            FROM replies r
            JOIN reply_drafts rd ON rd.reply_id = r.id
            JOIN businesses b ON b.id = r.lead_id
            LEFT JOIN outreach_log ol ON ol.id = r.outreach_id
            WHERE r.id = ? AND rd.status = 'draft'
        """, (reply_id,))
        
        row = cursor.fetchone()
        if not row:
            return {
                'success': False,
                'error': f'No draft found for reply {reply_id} or already sent/skipped'
            }
        
        # Determine sender account
        sender_address = row['sender_address'] or row['original_sender_address']
        sender_name = row['sender_name'] or row['original_sender_name']
        
        # Find matching account
        accounts = _load_accounts()
        account = None
        for acc in accounts:
            if acc['address'] == sender_address:
                account = acc
                break
        
        if not account:
            # Fallback: use first available account
            if accounts:
                account = accounts[0]
                print(f"Warning: Original sender {sender_address} not found, using {account['address']}")
            else:
                return {'success': False, 'error': 'No email accounts configured'}
        
        # Prepare email content
        subject = row['draft_subject'] or f"Re: {row['original_subject']}"
        body = edited_body if edited_body else row['draft_body']
        to_address = row['lead_email']
        
        # Send the email
        try:
            send_email(
                to_address=to_address,
                subject=subject,
                body=body,
                account=account
            )
            
            # Mark as sent in database
            conn.execute(
                """UPDATE reply_drafts 
                   SET status='sent', sent_at=datetime('now'), 
                       sender_name=?, sender_address=?
                   WHERE id=?""",
                (account['name'], account['address'], row['draft_id'])
            )
            conn.commit()
            
            return {
                'success': True,
                'reply_id': reply_id,
                'lead_name': row['lead_name'],
                'to_address': to_address,
                'from_address': account['address'],
                'subject': subject,
                'body_preview': body[:100] + '...' if len(body) > 100 else body
            }
            
        except Exception as e:
            # Mark as failed
            conn.execute(
                """UPDATE reply_drafts 
                   SET status='failed', error_note=?
                   WHERE id=?""",
                (str(e), row['draft_id'])
            )
            conn.commit()
            
            return {
                'success': False,
                'error': f'Failed to send email: {str(e)}',
                'reply_id': reply_id
            }
            
    except Exception as e:
        return {'success': False, 'error': f'Database error: {str(e)}'}
    finally:
        conn.close()


def get_pending_reply_drafts(client_id: Optional[str] = None, limit: int = 20) -> list:
    """
    Get pending reply drafts awaiting approval.
    
    Args:
        client_id: Filter by client (None for all/operator)
        limit: Maximum number to return
        
    Returns:
        List of pending drafts with lead info
    """
    conn = connect()
    try:
        if client_id:
            cursor = conn.execute("""
                SELECT 
                    r.id as reply_id,
                    r.from_address as lead_email,
                    r.content as lead_message,
                    r.received_at,
                    rd.subject,
                    rd.body as draft_body,
                    rd.rationale,
                    b.name as lead_name,
                    b.category,
                    rc.label as classification
                FROM replies r
                JOIN reply_drafts rd ON rd.reply_id = r.id
                JOIN businesses b ON b.id = r.lead_id
                LEFT JOIN reply_classification rc ON rc.reply_id = r.id
                WHERE rd.status = 'draft' AND r.client_id = ?
                ORDER BY r.received_at DESC
                LIMIT ?
            """, (client_id, limit))
        else:
            # Operator sees all unassigned or their own
            cursor = conn.execute("""
                SELECT 
                    r.id as reply_id,
                    r.from_address as lead_email,
                    r.content as lead_message,
                    r.received_at,
                    rd.subject,
                    rd.body as draft_body,
                    rd.rationale,
                    b.name as lead_name,
                    b.category,
                    rc.label as classification
                FROM replies r
                JOIN reply_drafts rd ON rd.reply_id = r.id
                JOIN businesses b ON b.id = r.lead_id
                LEFT JOIN reply_classification rc ON rc.reply_id = r.id
                WHERE rd.status = 'draft'
                ORDER BY r.received_at DESC
                LIMIT ?
            """, (limit,))
        
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


if __name__ == "__main__":
    # Test sending a reply
    import sys
    if len(sys.argv) > 1:
        reply_id = int(sys.argv[1])
        result = send_reply_email(reply_id)
        print(f"Result: {result}")
    else:
        # List pending
        pending = get_pending_reply_drafts()
        print(f"Pending drafts: {len(pending)}")
        for p in pending:
            print(f"  [{p['reply_id']}] {p['lead_name']}: {p['subject']}")
