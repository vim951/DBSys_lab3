# DBSys_lab3
Repository used to report the work of Hugo DANET and VIctor MASIAK for part 3 of the EURECOM DBSys lab on Java Minibase.

# Read txt files

We have coded two functions that allow to create conditional expressions for simple predicate and double predicates join, from the .txt query files. 
We have also implemented two functions allowing us to read the tables and the columns of the files.

To test the implementations, you can uncomment the queries in the `runTests` function and indicate the path to the queries folder in the String constant `path_to_queries`.

# Important classes to download

All the queries are executed in `JoinTest.java`
It is important to download our own version of `Tuple.java` since we implemented a custom `toString` function to write queries outputs into files.
Tasks 1a and 1b are implemented in `JoinTest.java`

Task 2a is implemented in the class `IESelfJoinSinglePredicate.java`.
Task 2b is implemented in the class `IESelfJoinDoublePredicate.java`.
Task 2c is implemented in the class `IEJoinDoublePredicate.java`.

These 3 classes have to be put into the iterator package.
