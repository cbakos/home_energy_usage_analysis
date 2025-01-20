CREATE TABLE IF NOT EXISTS "meters" (
  "meter_identifier" varchar PRIMARY KEY,
  "connection_type" varchar NOT NULL,
  "start_date" date NOT NULL,
  "end_date" date
);

CREATE TABLE IF NOT EXISTS "usage_entries" (
  "meter_identifier" varchar NOT NULL,
  "time" TIMESTAMPTZ NOT NULL,
  "reading_time_type" varchar,
  "reading_subtype" varchar,
  "value" decimal,
  "cumulative_reading" decimal,
  "temperature" decimal,
  PRIMARY KEY ("meter_identifier", "time", "reading_subtype"),
  CONSTRAINT fk_meter FOREIGN KEY ("meter_identifier") REFERENCES "meters"("meter_identifier") ON DELETE CASCADE
);


COMMENT ON COLUMN "usage_entries"."reading_time_type" IS 'low or high for electricity';

COMMENT ON COLUMN "usage_entries"."reading_subtype" IS 'Subtype: e.g., delivery or return)';

COMMENT ON COLUMN "usage_entries"."value" IS 'Value of the specific reading (e.g., 0.78)';

COMMENT ON COLUMN "usage_entries"."cumulative_reading" IS 'Cumulative reading (e.g., 16,196.41), nullable';

COMMENT ON COLUMN "usage_entries"."temperature" IS 'Temperature in Celsius at the time of the reading, nullable';

ALTER TABLE "usage_entries" ADD FOREIGN KEY ("meter_identifier") REFERENCES "meters" ("meter_identifier");
