#Input File:  input-f.txt

The input ﬁle contains the adjacency list and has multiple lines in the following format: User TAB Connections

Here, User is a unique integer ID corresponding to a unique user and Connections is a comma-separated list of unique IDs corresponding to the connections of the user with the unique ID <User>. Note that the connections are mutual (e.g., if A is connected with B then B is also connected with A). The data provided is consistent with that rule as there is an entry for each side of each connection.

#Approach:

Create a Scala Spark program that does the following:

Use a simple approach such that, for each user X, the program recommends ten users who are not already connections with X, but have the largest number of mutual connections in common with X.  Note, it is possible that X may get less than ten recommendations (even zero); since you only need to look at mutual connections.  (Solution in Python and MapReduce is the code file:  rec-f.py)

#Output:

The output should contain one line per user in the following format: User TAB Recommendations

where User is a unique ID corresponding to a user and Recommendations is a comma-separated list of unique IDs corresponding to the program’s recommendation of people that User might know, ordered by decreasing number of mutual connections. Even if a user has fewer than 10 second-degree connections, output all of them in decreasing order of the number of mutual connections. If a user has no connections, you can provide an empty list of recommendations. If there are multiple users with the same number of mutual connections, ties are broken by ordering them in a numerically ascending order of their user IDs.
