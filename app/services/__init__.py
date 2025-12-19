"""
Services Package

This package contains business logic and service layer code.
Services handle the core business logic and interact with the database layer.

Example structure:
- app/services/user_service.py - User business logic
- app/services/product_service.py - Product business logic

To add a new service:
1. Create a new file in this directory (e.g., app/services/user_service.py)
2. Define service classes/functions that contain business logic
3. Services should be called from API route handlers
4. Services should use the database layer (app/db/) for data access

Example:
    class UserService:
        def __init__(self, db):
            self.db = db
        
        async def get_user(self, user_id: int):
            # Business logic here
            return await self.db.get_user(user_id)
"""

