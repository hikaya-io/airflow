# Survey modelisation

## Survey
How is a `Survey` modeled?

Has the following attributes:

- `fields`: array of `Field` objects
- `form_id`: ID of the form
- `last_date`: ?
- `name`: Name of Survey (¿Not human friendly?)
- `statuses`: ¿What do they stand for?
- `unique_column`: ¿Is it really unique? ¿How does it differ from `form_id` above?

## Field

The `Field` object stands for a single field of the survey/questionnaire.

¿Can a field have subfields?

A `Field` has the following attributes:

- `name`: name of the Field. ¿Not human friendly?
- `type`: specifies the type of the Field. ¿What are the possible/supported types? `char`, `integer`, `timestamp`, `text` 