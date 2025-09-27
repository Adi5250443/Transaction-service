from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime, timezone
import asyncio
import redis
import json
import uvicorn
import os
from typing import Optional

# Redis setup with environment variables (more secure for production)
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis-16262.c270.us-east-1-3.ec2.redns.redis-cloud.com"),
    port=int(os.getenv("REDIS_PORT", 16262)),
    decode_responses=True,
    username=os.getenv("REDIS_USERNAME", "default"),
    password=os.getenv("REDIS_PASSWORD", "gbhZzNKGiDtnkdhdjJ0FEkjtX8qEpfwT"),
)

# Models
class TransactionWebhook(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str

class TransactionResponse(BaseModel):
    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str
    status: str
    created_at: str
    processed_at: Optional[str] = None

# FastAPI app
app = FastAPI()

async def process_transaction(transaction_id: str):
    """Background processing with 30-second delay"""
    await asyncio.sleep(30)  # Simulate external API calls
    
    # Update status to PROCESSED
    transaction_data = redis_client.get(f"tx:{transaction_id}")
    if transaction_data:
        tx = json.loads(transaction_data)
        tx["status"] = "PROCESSED"
        tx["processed_at"] = datetime.now(timezone.utc).isoformat()
        redis_client.set(f"tx:{transaction_id}", json.dumps(tx))

@app.get("/")
async def health_check():
    """Health check endpoint"""
    try:
        # Test Redis connection
        redis_client.ping()
        return {
            "status": "HEALTHY",
            "current_time": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Redis connection failed: {str(e)}")

@app.post("/v1/webhooks/transactions")
async def receive_webhook(transaction: TransactionWebhook, background_tasks: BackgroundTasks):
    """Receive webhook and process in background"""
    
    # Check if transaction already exists (idempotency)
    if redis_client.exists(f"tx:{transaction.transaction_id}"):
        return {"message": "Transaction already received"}, 202
    
    # Store transaction with PROCESSING status
    tx_data = {
        "transaction_id": transaction.transaction_id,
        "source_account": transaction.source_account,
        "destination_account": transaction.destination_account,
        "amount": transaction.amount,
        "currency": transaction.currency,
        "status": "PROCESSING",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "processed_at": None
    }
    
    redis_client.set(f"tx:{transaction.transaction_id}", json.dumps(tx_data))
    
    # Start background processing
    background_tasks.add_task(process_transaction, transaction.transaction_id)
    
    return {"message": "Transaction received"}, 202

@app.get("/v1/transactions/{transaction_id}", response_model=TransactionResponse)
async def get_transaction(transaction_id: str):
    """Get transaction status"""
    transaction_data = redis_client.get(f"tx:{transaction_id}")
    
    if not transaction_data:
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    tx = json.loads(transaction_data)
    return TransactionResponse(**tx)

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)