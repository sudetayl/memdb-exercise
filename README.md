# memdb-exercise
Coding exercise to implement a simple in memory database which allows nested transactions.  Commands can be provided either from standard in or from a file redirected to standard in.

The following commands are available

|Command | Description |
|:-------|:------------|
|GET key |print value for given key|
|SET key value|associate value with given key|
|NUMEQUALTO value|print number of keys with given value|
|NAMESEQUALTO value|print keys with given value|
|BEGIN|start transaction -- transactions can be nested|
|ROLLBACK|rollback current transaction.  Will print out, if no such transaction.|
|COMMIT|commit changes in all active transactions. All transactions closed.  Will print out, if not in transaction.|
|END|Terminate program|

## Requirements
Java 1.7 or later

## Compilation
javac memdb\Memdb.java

## Execution
java memdb.MemDb

java memdb.MemDb <  ..\resources\test\DbTestInput

## Test Input
A test input file can be found at
	resources/test/DbTestInput.txt

A file with the expected out from running the provided test input file can be found at
	resources/test/DbTestOutput.txt
