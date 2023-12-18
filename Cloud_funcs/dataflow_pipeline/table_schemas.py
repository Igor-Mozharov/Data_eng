costs_schema = {
    "fields": [
        {"name": "ad_group", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "keyword", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "medium", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "landing_page", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "channel", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "ad_content", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "campaign", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]},
        {"name": "location", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "cost", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]}
    ]
}

events_schema = {
    'fields': [
        {'name': 'user_params', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
            {'name': 'is_active_user', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'dclid', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'medium', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'marketing_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'term', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'campaign_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'context', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'gclid', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'transaction_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'specification', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'srsltid', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'model_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'os', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]},
        {'name': 'install_store', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'analytics_storage', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'engagement_time_msec', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'event_origin', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'localization_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'session_number', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'event_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'user_action_detail', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'model_number', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'place', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'numeric', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'official_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ga_session_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'current_progress', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'flag', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'alpha_3', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'browser', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'specification', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'selection', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
        {'name': 'alpha_2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'os', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}

installs_schema = {
    "fields": [
        {"name": "install_time", "type": "TIMESTAMP"},
        {"name": "marketing_id", "type": "STRING"},
        {"name": "channel", "type": "STRING"},
        {"name": "medium", "type": "STRING"},
        {"name": "campaign", "type": "STRING"},
        {"name": "keyword", "type": "STRING"},
        {"name": "ad_content", "type": "STRING"},
        {"name": "ad_group", "type": "STRING"},
        {"name": "landing_page", "type": "STRING"},
        {"name": "sex", "type": "STRING"},
        {"name": "alpha_2", "type": "STRING"},
        {"name": "alpha_3", "type": "STRING"},
        {"name": "flag", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "numeric", "type": "INTEGER"},
        {"name": "official_name", "type": "STRING"}
    ]
}

orders_schema = {
    'fields': [
        {'name': 'event_time', 'type': 'INTEGER'},
        {'name': 'transaction_id', 'type': 'STRING'},
        {'name': 'type', 'type': 'STRING'},
        {'name': 'origin_transaction_id', 'type': 'STRING'},
        {'name': 'category', 'type': 'STRING'},
        {'name': 'payment_method', 'type': 'STRING'},
        {'name': 'fee', 'type': 'FLOAT'},
        {'name': 'tax', 'type': 'FLOAT'},
        {'name': 'iap_item_name', 'type': 'STRING'},
        {'name': 'iap_item_price', 'type': 'FLOAT'},
        {'name': 'discount_code', 'type': 'STRING'},
        {'name': 'discount_amount', 'type': 'FLOAT'},
    ]
}