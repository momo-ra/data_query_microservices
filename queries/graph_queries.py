from sqlalchemy import text
CREATE_GRAPH = text("""
    INSERT INTO graph_type (name, description, created_at, updated_at)
    VALUES (:graph_name, :description, :created_at, :updated_at)
    RETURNING id
                    """)