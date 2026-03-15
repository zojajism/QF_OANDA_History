-- Creates a stored procedure that merges missing rows from public.candles_stage
-- into public.candles using the business key:
-- (exchange, symbol, timeframe, close_time)

CREATE OR REPLACE PROCEDURE public.merge_candles_stage_to_candles()
LANGUAGE plpgsql
AS $$
DECLARE
    v_inserted integer := 0;
BEGIN
    -- Insert only keys that do not already exist in the main table.
    -- DISTINCT ON protects against duplicates inside staging itself.
    INSERT INTO public.candles (
        exchange,
        symbol,
        timeframe,
        open_time,
        close_time,
        base_currency,
        quote_currency,
        open,
        high,
        low,
        close,
        volume,
        message_datetime
    )
    SELECT
        s.exchange,
        s.symbol,
        s.timeframe,
        s.open_time,
        s.close_time,
        s.base_currency,
        s.quote_currency,
        s.open,
        s.high,
        s.low,
        s.close,
        s.volume,
        COALESCE(s.message_datetime, s.record_time)
    FROM (
        SELECT DISTINCT ON (exchange, symbol, timeframe, close_time)
            exchange,
            symbol,
            timeframe,
            open_time,
            close_time,
            base_currency,
            quote_currency,
            open,
            high,
            low,
            close,
            volume,
            message_datetime,
            record_time,
            id
        FROM public.candles_stage
        ORDER BY exchange, symbol, timeframe, close_time, id DESC
    ) AS s
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.candles c
        WHERE c.exchange = s.exchange
          AND c.symbol = s.symbol
          AND c.timeframe = s.timeframe
          AND c.close_time = s.close_time
    );

    GET DIAGNOSTICS v_inserted = ROW_COUNT;
    RAISE NOTICE 'merge_candles_stage_to_candles: inserted % row(s)', v_inserted;
END;
$$;

-- Optional but recommended for performance of NOT EXISTS checks:
CREATE INDEX IF NOT EXISTS idx_candles_key_lookup
    ON public.candles (exchange, symbol, timeframe, close_time);
