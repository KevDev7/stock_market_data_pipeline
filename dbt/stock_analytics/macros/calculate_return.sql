-- Calculates a percentage return over a given number of periods for use in models.
{% macro calculate_return(periods, partition_key='ticker') %}
    CASE 
        WHEN COUNT(close) OVER (
            PARTITION BY {{ partition_key }}
            ORDER BY trade_date
            ROWS BETWEEN {{ periods - 1 }} PRECEDING AND CURRENT ROW
        ) >= {{ periods }}
        THEN
            IFF(
                LAG(close, {{ periods }}) OVER (PARTITION BY {{ partition_key }} ORDER BY trade_date) != 0,
                (close - LAG(close, {{ periods }}) OVER (PARTITION BY {{ partition_key }} ORDER BY trade_date))
                / LAG(close, {{ periods }}) OVER (PARTITION BY {{ partition_key }} ORDER BY trade_date),
                NULL
            )
        ELSE NULL
    END
{% endmacro %}
