FROM {{ ref('stg_calls') }} c
LEFT JOIN {{ source('megaline', 'users') }} u
    ON c.user_id = u.user_id
LEFT JOIN {{ source('megaline', 'plans') }} p
    ON u.plan = p.plan_name

UNION ALL

SELECT *
FROM {{ ref('stg_messages') }} m
LEFT JOIN {{ source('megaline', 'users') }} u
    ON m.user_id = u.user_id
LEFT JOIN {{ source('megaline', 'plans') }} p
    ON u.plan = p.plan_name

UNION ALL

SELECT *
FROM {{ ref('stg_internet') }} i
LEFT JOIN {{ source('megaline', 'users') }} u
    ON i.user_id = u.user_id
LEFT JOIN {{ source('megaline', 'plans') }} p
    ON u.plan = p.plan_name