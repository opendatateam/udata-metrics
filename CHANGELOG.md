# Changelog

## Current (in progress)

- Update translations [#13](https://github.com/opendatateam/udata-metrics/pull/13)

## 2.0.1 (2023-12-01)

- Improve metrics layout [#11](https://github.com/opendatateam/udata-metrics/pull/11)
    - Specify that statistics are for the year
    - Move datasets graph on the second row in dashboard stats
    - Use current month value (and not variation)
    - Prevent upper or lowercasing by removing any text transformation


## 2.0.0 (2023-11-21)

- :warning: New metrics logic [#8](https://github.com/opendatateam/udata-metrics/pull/8)
    - Add metrics views (using template hook)
    - Use optional metrics API (with job to update metrics on objects)
    - Remove Influx logic
- Replace mongo legacy image in CI [#6](https://github.com/opendatateam/udata-metrics/pull/6)

## 1.0.2 (2020-07-01)

- Add time condition in the queries to retrieve only last 24 hours results [#4](https://github.com/opendatateam/udata-metrics/pull/4)

## 1.0.1 (2020-05-13)

- Nothing yet

## 1.0.0 (2020-05-12)

Initial release
