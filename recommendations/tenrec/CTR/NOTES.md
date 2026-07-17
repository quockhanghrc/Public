# CTR Data 1M Column Schema

| # | Column | Type | Description |
|---|---|---|---|
| 1 | `user_id` | int | Unique user identifier |
| 2 | `item_id` | int | Unique item (video) identifier |
| 3 | `click` | int (0/1) | **Target**: whether user clicked |
| 4 | `follow` | int (0/1) | Whether user followed the source |
| 5 | `like` | int (0/1) | Whether user liked the item |
| 6 | `share` | int (0/1) | Whether user shared the item |
| 7 | `short_v` | int (0/1) | Whether it's a short video |
| 8 | `play_times` | int | Number of times played |
| 9 | `gender` | int | User gender (coded) |
| 10 | `age` | int | User age (coded) |
| 11 | `hist_1` | int | 1st most recent clicked item |
| 12 | `hist_2` | int | 2nd most recent clicked item |
| 13 | `hist_3` | int | 3rd most recent clicked item |
| 14 | `hist_4` | int | 4th most recent clicked item |
| 15 | `hist_5` | int | 5th most recent clicked item |
| 16 | `hist_6` | int | 6th most recent clicked item |
| 17 | `hist_7` | int | 7th most recent clicked item |
| 18 | `hist_8` | int | 8th most recent clicked item |
| 19 | `hist_9` | int | 9th most recent clicked item |
| 20 | `hist_10` | int | 10th most recent clicked item |