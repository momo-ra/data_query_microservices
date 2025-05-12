from queries.tag_queries import (
    get_tag_data_with_tag_id,
    get_all_tag_data,
    get_trends_data,
    get_historical_tag_data,
    get_polling_tags
)

from queries.table_queries import get_table_data

from queries.card_queries import (
    get_user_cards,
    create_user_card,
    update_user_card,
    delete_card
)

from queries.dashboard_queries import get_user_active_cards

from queries.graph_queries import create_graph
