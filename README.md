# Large Scale Data Processing: Project 3
**Rafael Espinoza and Ahmad Choudry**

## 1. Test Results

## Results of verifyMIS Function

| Graph file             | MIS file                  | Is an MIS? |
|------------------------|---------------------------|------------|
| small_edges.csv        | small_edges_MIS.csv       | Yes        |
| small_edges.csv        | small_edges_non_MIS.csv   | No         |
| line_100_edges.csv     | line_100_MIS_test_1.csv   | Yes        |
| line_100_edges.csv     | line_100_MIS_test_2.csv   | No         |
| twitter_10000_edges.csv| twitter_10000_MIS_test_1.csv | No      |
| twitter_10000_edges.csv| twitter_10000_MIS_test_2.csv | Yes     |


## 2. Results of LubyMIS Function

| Graph file             | Output File               | Success? | Iterations | Seconds |
|------------------------|---------------------------|----------|------------|---------|
| small_edges.csv        | small_edges_personal.csv  | Yes      | 1          | 2s      |
| line_100_edges.csv     | line_100_personal.csv     | Yes      | 2          | 5s      |
| twitter_100_edges.csv  | twitter_100_personal.csv  | Yes      | 3          | 10s     |
| twitter_1000_edges.csv | twitter_1000_personal.csv | Yes      | 5          | 30s     |
| twitter_10000_edges.csv| twitter_10000_personal.csv| Yes      | 8          | 120s    |

## 3. Results of GCP

### GCP Run on twitter_original_edges.csv with 4x2 Cores

| Graph file             | Output File                         | Success? | Iterations | Seconds |
|------------------------|-------------------------------------|----------|------------|---------|
| twitter_original_edges.csv | gcp_twitter_original_4x2_personal.csv | Yes      | 15         | 700s    |

### GCP Run on twitter_original_edges.csv with 2x2 Cores

| Graph file             | Output File                         | Success? | Iterations | Seconds |
|------------------------|-------------------------------------|----------|------------|---------|
| twitter_original_edges.csv | gcp_twitter_original_2x2_personal.csv | Yes      | 15         | 800s    |
