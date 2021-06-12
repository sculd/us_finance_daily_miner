# About
This projects ingests daily stock price and performs outlier analysis. The anlysis is published on daily basis as an e-mail.

# Build & Deploy

### Build
```
python run build
```

### Deploy
```
python run deploy
```

## Architecture

```
                                                                                  +------------------------+
                                                                                  |                        |
                                                                                  |        GCP BigQuery    |
                                                                                  |                        |
                                      +-------------------+        update daily   |                        |
                                      |                   |           +-----------+-> daily_snp500         |
                             +--------+> /dailysnp500 ----+-------+---+           |                        |
+-------------------+        |        |                   |       |   +-----------+-> daily                |
|                   +--------+        |                   |       |               |                        |
| Cloud Scheduler   |                 |                   |       +---------------+-> monthly              |
|                   +--------+        |                   |        update monthly |                        |
+-------------------+        |        |                   |                       |                        |
                             +-------->  /outlier_analysis+------------+          +------------------------+
                                      |                   |            |
                      Daily request   +-------------------+            |
                                                                       |           +----------------------+
                                                                       |           |                      |
                                                                       +-----------+---> User             |
                                                                                   +----------------------+
                                                                        e-mail


```