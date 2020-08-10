This project focuses on the architecture and computer science concepts involved in building a 
Database Management System (DBMS) from the ground up. It is essentially the backend of a 
PostgreSQL server. This project was completed through UC Berkeley's Introduction to Database 
Management Systems course. 

The first part of the project focuses on implementing an index structure to maintain data on 
disk, specifically we use an Alternative 2 B+ Tree. You can find the code for this section 
in src/main/java/edu/berkeley/cs186/database/index.


Next, we move onto the backend implementation of joins and query optimization. Join algorithms 
are explored, including Page Nested Loop Joins, Sort Merge Join, and Grace Hash Join and for 
query optimization we perform plan space searches. You can find the code for this section in
src/main/java/edu/berkeley/cs186/database/query.

The third part of the DBMS project implements concurrency in our database through locks and transactions. 
You can find the code for this section in src/main/java/edu/berkeley/cs186/database/concurrency.

The final portion of this project deals with recovering state in the database. We use forward 
processing where we perform logging and maintain some metadata during normal operation of the 
database, and restart recovery (a.k.a. crash recovery), which is the processes taken when the 
database starts up again. You can find the code for this section in 
src/main/java/edu/berkeley/cs186/database/recovery. 




