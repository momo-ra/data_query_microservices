from fastapi import WebSocket
from utils.log import setup_logger
from fastapi.websockets import WebSocketState
import time


logger = setup_logger(__name__)

class WebSocketManager:
    def __init__(self):
        self.active_users: dict[str, WebSocket] = {}
    
    #connect to websocket manager
    async def connect(self, websocket:WebSocket, userId):
        await websocket.accept()
        if userId:
            self.active_users[userId] = websocket
            logger.info(f"User {userId} connected to websocket")
            return userId

    #disconnect from websocket
    async def disconnect(self, user_id):
        if user_id in self.active_users:
            try:
                # Get the websocket before removing it from active_users
                websocket = self.active_users[user_id]
                
                # Remove from tracking
                self.active_users.pop(user_id)
                logger.info(f"User {user_id} disconnected from websocket")
                
                # Actively close the connection if it's still open
                try:
                    await websocket.close(code=1000, reason="Session ended")
                    logger.info(f"WebSocket connection closed for user {user_id}")
                except RuntimeError as e:
                    # Connection might already be closed
                    logger.warning(f"Error closing websocket for user {user_id}: {str(e)}")
                
                return True
            except Exception as e:
                logger.error(f"Error in disconnect for user {user_id}: {str(e)}")
                return False
        return False

    #send message to active user
    async def send_message(self, message, active_user, websocket:WebSocket):
        try:
            # Check if user is active and connection is open
            if active_user in self.active_users:
                # Check WebSocket state before sending - only check DISCONNECTED state
                if websocket.client_state == WebSocketState.DISCONNECTED:
                    logger.warning(f"Cannot send message to user {active_user} - connection is closed")
                    # Remove user if connection is closed
                    await self.disconnect(active_user)
                    return False
                await websocket.send_json(message)
                return True
            else:
                logger.warning(f"Attempted to send message to inactive user: {active_user}")
                return False
        except Exception as e:
            logger.error(f"Error sending message to user {active_user}: {e}")
            # If error indicates connection is closing, disconnect user
            if "close message has been sent" in str(e):
                logger.info(f"Connection is closing for user {active_user}, removing from active users")
                await self.disconnect(active_user)
            return False
    
    # Check if a user is connected
    def is_connected(self, user_id):
        return user_id in self.active_users
    
    # Get total active connections
    def get_connection_count(self):
        return len(self.active_users)

websocket_manager = WebSocketManager()