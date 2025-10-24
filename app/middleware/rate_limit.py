from fastapi import Request, HTTPException, status
from app.db.redis_client import redis_client
import time


async def rate_limit_middleware(request: Request, call_next):
    if request.url.path == "/events":
        client_ip = request.client.host
        key = f"rate_limit:{client_ip}:{int(time.time() / 60)}"

        current = await redis_client.incr(key)

        if current == 1:
            await redis_client.expire(key, 60)

        from app.config import settings
        if current > settings.rate_limit_per_minute:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded"
            )

    response = await call_next(request)
    return response