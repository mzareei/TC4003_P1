#!/bin/bash
go run ../main/ii.go master sequential ../main/pg-*.txt
sort -k1,1 mrtmp.iiseq | sort -snk2,2 | grep -v '16' | tail -10 | diff - ../main/mr-challenge.txt > ../main/diff.out
if [ -s ../main/diff.out ]
then
echo "Failed test. Output should be as in mr-challenge.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat ../main/diff.out
else
  echo "Passed test" > /dev/stderr
fi

