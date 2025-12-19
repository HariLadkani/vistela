"""
API Routes Package

This package contains all API route handlers.
Organize routes by domain/resource (e.g., users.py, products.py, etc.)

Example structure:
- app/api/users.py - User-related endpoints
- app/api/products.py - Product-related endpoints

To add a new route:
1. Create a new file in this directory (e.g., app/api/users.py)
2. Define a router using: router = APIRouter(prefix="/users", tags=["users"])
3. Add your endpoints to the router
4. Import and include the router in app/main.py:
   from app.api.users import router as users_router
   app.include_router(users_router)
"""

