from typing import List, Dict, Any, Optional
from datetime import datetime
from utils.log import setup_logger

logger = setup_logger(__name__)

def format_dashboard_cards(cards: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Format card data specifically for dashboard consumption"""
    result = {
        "cards": [],
        "tag_mappings": {},
        "config": {
            "refresh_interval": 5000,
            "default_graph_type": 1
        }
    }
    
    # Process cards into the exact format needed by dashboard
    for card in cards:
        dashboard_card = {
            "id": card["id"],
            "time_range": {
                "start": card["start_time"],
                "end": card["end_time"]
            },
            "is_active": card["is_active"],
            "tag_ids": [tag["id"] for tag in card["tags"]],
            "display_settings": {
                "graph_type": card.get("graph_type_id", 1),
                "show_legend": True,
                "colors": ["#4285F4", "#34A853", "#FBBC05", "#EA4335"]
            }
        }
        result["cards"].append(dashboard_card)
        
        # Build tag mappings for quick lookup
        for tag in card["tags"]:
            if str(tag["id"]) not in result["tag_mappings"]:
                result["tag_mappings"][str(tag["id"])] = tag["name"]
    
    return result

def format_realtime_update(tag_id: str, value: Any, timestamp: datetime, tag_name: Optional[str] = None) -> Dict[str, Any]:
    """Format a real-time update for dashboard consumption"""
    try:
        # Format numeric values nicely
        formatted_value = f"{float(value):.2f}" if isinstance(value, (int, float)) else str(value)
    except (ValueError, TypeError):
        formatted_value = str(value)
        
    # Format timestamp for display
    formatted_time = timestamp.strftime("%H:%M:%S") if isinstance(timestamp, datetime) else str(timestamp)
    
    return {
        "tag_id": tag_id,
        "value": value,
        "tag_name": tag_name,
        "timestamp": timestamp,
        "formatted": {
            "value": formatted_value,
            "time": formatted_time
        }
    }


def format_historical_data(data: List[Dict[str, Any]], tag_to_card_map: Dict[str, int] = None) -> Dict[str, Any]:
    """
    Format historical data for dashboard charts
    
    Parameters:
    - data: List of data points
    - tag_to_card_map: Optional mapping of tag_ids to card_ids (which tag belongs to which card)
    """
    result = {
        "series": {},
        "summary": {},
        "card_mappings": {} if tag_to_card_map else None  # Include card mappings if provided
    }
    
    # Add tag to card mapping if provided
    if tag_to_card_map:
        for tag_id, card_id in tag_to_card_map.items():
            if str(tag_id) not in result["card_mappings"]:
                result["card_mappings"][str(tag_id)] = card_id
    
    # Group by tag_id
    for item in data:
        # Handle case where item might be a string instead of dict
        if not isinstance(item, dict):
            logger.warning(f"Received non-dictionary item in historical data: {item}")
            continue
            
        tag_id = str(item.get("tag_id", ""))
        if not tag_id:
            logger.warning(f"Item missing tag_id: {item}")
            continue
            
        if tag_id not in result["series"]:
            result["series"][tag_id] = {
                "name": item.get("tag_name", f"Tag {tag_id}"),
                "data": [],
                "card_id": tag_to_card_map.get(tag_id) if tag_to_card_map else None
            }
            # Initialize summary statistics
            result["summary"][tag_id] = {
                "min": float('inf'),
                "max": float('-inf'),
                "avg": 0,
                "count": 0,
                "sum": 0,
                "card_id": tag_to_card_map.get(tag_id) if tag_to_card_map else None
            }
        
        # Add datapoint
        try:
            value = float(item.get("value", 0))
            timestamp = item.get("timestamp")
            
            if timestamp is None:
                logger.warning(f"Missing timestamp for item: {item}")
                continue
                
            # Add to series
            result["series"][tag_id]["data"].append({
                "x": timestamp,
                "y": value
            })
            
            # Update summary
            summary = result["summary"][tag_id]
            summary["min"] = min(summary["min"], value)
            summary["max"] = max(summary["max"], value)
            summary["sum"] += value
            summary["count"] += 1
        except (ValueError, TypeError) as e:
            # Skip non-numeric values
            logger.warning(f"Could not process item value: {item.get('value')} - {e}")
            pass
    
    # Calculate averages
    for tag_id, summary in result["summary"].items():
        if summary["count"] > 0:
            summary["avg"] = summary["sum"] / summary["count"]
            
        # Format for display
        summary["min"] = f"{summary['min']:.2f}" if summary["min"] != float('inf') else "N/A"
        summary["max"] = f"{summary['max']:.2f}" if summary["max"] != float('-inf') else "N/A"
        summary["avg"] = f"{summary['avg']:.2f}" if summary["count"] > 0 else "N/A"
    
    return result 