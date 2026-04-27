# Step B — OOS Kill-test (2016-2025)

**Council R2 criteria**: 新 champion `Dual + tv_top_25 + only_volatile + top_20` 必須在 2016-2019 OOS Sharpe > 0.8、2022 MDD 不破 -40%、排名相對其他 mcap 不翻盤。

**資料延伸**: 從 `trade_journal_value_tw_long_snapshot.parquet` (2016-01 起)

## 1. FULL 2016-2025 grid（排序 by Sharpe）

| strategy | mcap | regime | CAGR% | Sharpe | MDD% | hit% | v_on% | n |
|---|---|---|---|---|---|---|---|---|
| Dual-all-only_volatile | all | only_volatile | 16.99 | 0.966 | -17.52 | 55.8 | 18.6 | 129 |
| Dual-tv_top_25-only_volatile | tv_top_25 | only_volatile | 17.4 | 0.925 | -25.18 | 58.4 | 18.4 | 125 |
| Dual-all-none | all | none | 20.81 | 0.831 | -29.65 | 59.7 | 100.0 | 129 |
| Dual-tv_top_50-only_volatile | tv_top_50 | only_volatile | 16.1 | 0.798 | -24.59 | 53.5 | 17.8 | 129 |
| Dual-tv_top_75-only_volatile | tv_top_75 | only_volatile | 15.51 | 0.786 | -25.13 | 54.3 | 17.8 | 129 |
| Dual-tv_top_25-none | tv_top_25 | none | 17.8 | 0.587 | -48.77 | 56.8 | 100.0 | 125 |
| Dual-tv_top_75-none | tv_top_75 | none | 16.02 | 0.57 | -41.96 | 59.7 | 100.0 | 129 |
| Dual-tv_top_50-none | tv_top_50 | none | 14.76 | 0.498 | -44.15 | 59.7 | 100.0 | 129 |

## 2. Period breakdown (OOS vs IS vs Bear)

### CAGR% by period

period                   BEAR_2018_Q4  BEAR_2022  IS_2020_2025  OOS_2016_2019
mcap      regime                                                             
all       none                  95.42      -7.16         22.94          18.13
          only_volatile         46.50       5.68         20.50          12.27
tv_top_25 none                  21.64     -42.86         17.49          18.65
          only_volatile         25.36     -14.33         17.44          17.71
tv_top_50 none                  96.16     -37.70         11.37          20.37
          only_volatile         46.50     -15.52         18.77          12.52
tv_top_75 none                  90.95     -34.56         14.22          19.12
          only_volatile         46.50     -14.89         17.85          12.39

### Sharpe by period

period                   BEAR_2018_Q4  BEAR_2022  IS_2020_2025  OOS_2016_2019
mcap      regime                                                             
all       none                  6.208     -0.284         0.827          0.895
          only_volatile         3.577      0.203         1.028          0.941
tv_top_25 none                  0.724     -1.319         0.515          0.782
          only_volatile         1.456     -0.775         0.779          1.564
tv_top_50 none                  5.687     -1.280         0.330          0.919
          only_volatile         3.577     -0.840         0.795          0.942
tv_top_75 none                  5.653     -1.194         0.435          0.958
          only_volatile         3.577     -0.787         0.775          0.941

### MDD% by period

period                   BEAR_2018_Q4  BEAR_2022  IS_2020_2025  OOS_2016_2019
mcap      regime                                                             
all       none                    0.0     -28.29        -29.65         -27.56
          only_volatile           0.0     -15.06        -17.52         -14.43
tv_top_25 none                   -4.8     -48.77        -48.77         -34.65
          only_volatile           0.0     -25.18        -25.18          -9.87
tv_top_50 none                    0.0     -43.73        -44.15         -31.41
          only_volatile           0.0     -24.59        -24.59         -14.43
tv_top_75 none                    0.0     -41.96        -41.96         -28.36
          only_volatile           0.0     -25.13        -25.13         -14.43


## 3. Champion (`Dual-tv_top_25-only_volatile`) 年度分解

 year  n_rebal  ann_ret_pct  sharpe  mdd_pct  hit
 2016       13         7.77   1.061    -4.14 61.5
 2017       11        31.58   2.510    -2.31 90.9
 2018       13        10.48   0.707    -9.87 46.2
 2019       12        21.12   2.084    -2.20 66.7
 2020       12        29.37   1.698    -8.18 75.0
 2021       12        62.19   2.040    -3.15 58.3
 2022       13       -13.52  -0.734   -25.18 38.5
 2023       13        32.28   2.452    -1.87 61.5
 2024       13         3.42   0.131   -12.06 46.2
 2025       13         5.04   0.166   -12.31 46.2

## 4. Pass/Fail Verdict

| Criterion | Target | Actual | Pass? |
|---|---|---|---|
| 2016-2019 OOS Sharpe | > 0.8 | 1.564 | ✅ |
| 2022 MDD | > -40% | -25.18% | ✅ |
| 10-strategy Sharpe rank | Top 3 | #2 | ✅ |
