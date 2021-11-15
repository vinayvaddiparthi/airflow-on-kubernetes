from typing import Any, Dict

# view_id from GA: Overall - IP and spam filtered
VIEW_ID = "102376443"
ROW_LIMIT = 10000

reports: Dict[str, Any] = {
    "usr": {
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:sessions"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension6"},  # cid_ga
                        {"name": "ga:dimension5"},  # cid_platform
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        }
    },
    "ad_cost": {
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:adClicks"},
                        {"expression": "ga:adCost"},
                    ],
                    "dimensions": [
                        {"name": "ga:date"},
                        {"name": "ga:adwordsCampaignID"},
                        {"name": "ga:campaign"},
                        {"name": "ga:keyword"},
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        }
    },
    "cx": {
        "primary_keys": [
            "fields:dimension6::string",
            "fields:eventAction::string",
            "fields:dimension1::string",
        ],
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:users"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension6"},  # cid_ga
                        {"name": "ga:hostname"},
                        {"name": "ga:pagePath"},
                        {"name": "ga:eventAction"},
                        {"name": "ga:eventCategory"},
                        {"name": "ga:dimension1"},  # millisecond timestamp
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        },
    },
    "server_cx": {
        "primary_keys": [
            "fields:dimension5::string",
            "fields:eventAction::string",
            "fields:dimension1::string",
        ],
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:users"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension5"},  # cid_platform
                        {"name": "ga:hostname"},
                        {"name": "ga:pagePath"},
                        {"name": "ga:eventAction"},
                        {"name": "ga:eventCategory"},
                        {"name": "ga:dimension1"},  # millisecond timestamp
                        {"name": "ga:dateHourMinute"},
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        },
    },
    # TODO: delete once the transition to the new acquisition_funnel report is complete
    "acquisition": {
        "primary_keys": ["fields:dimension6::string", "fields:dimension1::string"],
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:newUsers"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension6"},  # cid_ga
                        {"name": "ga:sourceMedium"},
                        {"name": "ga:landingPagePath"},
                        {"name": "ga:fullReferrer"},
                        {"name": "ga:campaign"},
                        {"name": "ga:adwordsCampaignID"},
                        {"name": "ga:dimension1"},  # millisecond timestamp
                        {"name": "ga:keyword"},
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        },
    },
    "acquisition_funnel": {
        "primary_keys": ["fields:dimension6::string", "fields:dimension1::string"],
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:uniquePageViews"},
                    ],
                    "dimensions": [
                        {"name": "ga:dimension6"},  # cid_ga
                        {"name": "ga:sourceMedium"},
                        {"name": "ga:pagePath"},
                        {"name": "ga:campaign"},
                        {"name": "ga:adwordsCampaignID"},
                        {"name": "ga:keyword"},
                        {"name": "ga:dimension1"},  # millisecond timestamp
                    ],
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        },
    },
    "server_cx_email": {
        "payload": {
            "reportRequests": [
                {
                    "viewId": VIEW_ID,
                    "dateRanges": [{"startDate": None, "endDate": None}],
                    "metrics": [
                        {"expression": "ga:users"},
                    ],
                    "dimensions": [
                        {"name": "ga:eventLabel"},
                        {"name": "ga:hostname"},
                        {"name": "ga:pagePath"},
                        {"name": "ga:eventAction"},
                        {"name": "ga:eventCategory"},
                        {"name": "ga:dateHourMinute"},
                    ],
                    "filtersExpression": "ga:eventCategory==email",
                    "pageToken": "0",
                    "pageSize": ROW_LIMIT,
                }
            ]
        },
    },
}
