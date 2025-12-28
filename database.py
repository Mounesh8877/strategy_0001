import sqlite3
from datetime import datetime
import json

DB_NAME = "trading_bot.db"

def init_db():
    """Initializes the SQLite database with the advanced schema."""
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL;")
    
    # 1. TRADES TABLE (The Complete Metrics List)
    c.execute('''
        CREATE TABLE IF NOT EXISTS trades (
            id TEXT PRIMARY KEY,
            trade_id_legacy INTEGER, 
            symbol TEXT,
            strategy_name TEXT,
            status TEXT,
            side TEXT,
            
            -- Entry Context (The "Why")
            signal_time TEXT,
            entry_time TEXT,
            latency_ms INTEGER,
            regime TEXT,           
            adx REAL,
            atr REAL,
            volatility_pct REAL,
            obi_50 REAL,           
            volume_30m REAL,
            open_interest REAL,
            funding_rate REAL,
            z_score REAL,
            
            -- Risk & Setup
            leverage INTEGER,
            position_size REAL,
            entry_price REAL,
            requested_entry REAL,
            slippage REAL,
            initial_sl REAL,
            initial_tp REAL,
            planned_rrr REAL,
            dc_range REAL,
            
            -- Management
            stop_loss REAL,
            target REAL,
            highest_price REAL,
            lowest_price REAL,
            stop_loss_order_ids TEXT,   -- Stored as JSON string
            take_profit_order_ids TEXT, -- Stored as JSON string
            
            -- Performance (The "Result")
            exit_price REAL,
            exit_time TEXT,
            exit_reason TEXT,
            fees REAL,
            gross_pnl REAL,
            net_pnl REAL,
            
            -- Intra-Trade Health
            mae REAL,              
            mfe REAL,              
            duration_mins REAL
        )
    ''')
    
    # 2. BALANCE HISTORY
    c.execute('''
        CREATE TABLE IF NOT EXISTS balance_history (
            timestamp TEXT PRIMARY KEY,
            balance REAL,
            pnl_daily REAL
        )
    ''')
    
    conn.commit()
    conn.close()
    print("Database initialized successfully.")

def get_connection():
    return sqlite3.connect(DB_NAME)

def log_trade_entry(trade_data):
    """Inserts a new trade."""
    conn = get_connection()
    c = conn.cursor()
    
    # Convert lists to JSON strings for storage
    if 'stop_loss_order_ids' in trade_data:
        trade_data['stop_loss_order_ids'] = json.dumps(trade_data['stop_loss_order_ids'])
    if 'take_profit_order_ids' in trade_data:
        trade_data['take_profit_order_ids'] = json.dumps(trade_data['take_profit_order_ids'])

    keys = ', '.join(trade_data.keys())
    question_marks = ', '.join(['?'] * len(trade_data))
    sql = f"INSERT OR REPLACE INTO trades ({keys}) VALUES ({question_marks})"
    
    try:
        c.execute(sql, list(trade_data.values()))
        conn.commit()
    except Exception as e:
        print(f"DB Error inserting trade: {e}")
    finally:
        conn.close()

def update_trade(trade_id, update_data):
    """Updates an existing trade (e.g., SL update, Close)."""
    conn = get_connection()
    c = conn.cursor()
    
    # Convert lists to JSON strings
    if 'stop_loss_order_ids' in update_data:
        update_data['stop_loss_order_ids'] = json.dumps(update_data['stop_loss_order_ids'])
    if 'take_profit_order_ids' in update_data:
        update_data['take_profit_order_ids'] = json.dumps(update_data['take_profit_order_ids'])

    set_clause = ', '.join([f"{k} = ?" for k in update_data.keys()])
    values = list(update_data.values()) + [trade_id]
    
    sql = f"UPDATE trades SET {set_clause} WHERE id = ?"
    
    try:
        c.execute(sql, values)
        conn.commit()
    except Exception as e:
        print(f"DB Error updating trade {trade_id}: {e}")
    finally:
        conn.close()

def fetch_open_positions():
    """Loads open positions from DB to resume bot state."""
    conn = get_connection()
    conn.row_factory = sqlite3.Row # Access columns by name
    c = conn.cursor()
    
    try:
        c.execute("SELECT * FROM trades WHERE status = 'OPEN'")
        rows = c.fetchall()
        positions = []
        for row in rows:
            p = dict(row)
            # Restore JSON fields
            if p.get('stop_loss_order_ids'):
                p['stop_loss_order_ids'] = json.loads(p['stop_loss_order_ids'])
            if p.get('take_profit_order_ids'):
                p['take_profit_order_ids'] = json.loads(p['take_profit_order_ids'])
            
            # Map DB fields back to Bot's expected keys if they differ
            # (In this code, I aligned them, so it's direct mapping)
            positions.append(p)
        return positions
    except Exception as e:
        print(f"DB Error fetching open positions: {e}")
        return []
    finally:
        conn.close()

def log_balance(balance):
    conn = get_connection()
    c = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        c.execute("INSERT INTO balance_history (timestamp, balance) VALUES (?, ?)", (timestamp, balance))
        conn.commit()
    except Exception as e:
        print(f"DB Error logging balance: {e}")
    finally:
        conn.close()
