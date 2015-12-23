CREATE INDEX idx_calling_called_number ON camel_data (calling_number,called_number);
ANALYZE camel_data;
