CREATE TABLE IF NOT EXISTS Return (
    Code       TEXT (6)  CONSTRAINT code_not_null_con NOT NULL ON CONFLICT ROLLBACK,
    Name       TEXT      CONSTRAINT name_not_null_con NOT NULL ON CONFLICT ROLLBACK,
    TradingDay TEXT (10) CONSTRAINT td_not_null_con NOT NULL ON CONFLICT ROLLBACK,
    UnitNAV    NUMERIC   NOT NULL,
    CumNAV     NUMERIC   NOT NULL,
    Return     NUMERIC   NOT NULL,
    PRIMARY KEY (
        Code,
        Name,
        TradingDay
    )
)
WITHOUT ROWID;
