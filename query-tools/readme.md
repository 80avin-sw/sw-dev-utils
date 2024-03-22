## How to use
1. Create a directory `my-queries` in this directory, or anywhere
2. Put the queries inside `my-queries` directory
3. Follow [Example runs](#example-runs)


## Example runs
```shell
# search for queries recursively inside `./my-queries` and run each query 5 times
./bench-all.sh 5 ./my-queries


# search for queries recursively inside `./my-queries` and find query plan
./plan.js ./my-queries
# Next to every query, a file with name `<query-name>.plan.txt` will be created.
# Copy the contents and open as URL in a browser
```

### Open output in google sheet

run
```shell
./bench-all 5 ./my-queries | awk 'BEGIN {OFS="\t"} {$1=$1; print}' | pbcopy
# TSV contents added into clipboard. now paste into google sheets

# or on an existing output file,
cat ./my-queries/2024-03-22_17-16-31.txt | awk 'BEGIN {OFS="\t"} {$1=$1; print}' | pbcopy
```
