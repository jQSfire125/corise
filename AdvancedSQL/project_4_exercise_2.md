### Do you agree with the results returned by the query?
In general, yes. The query includes a filter on the self joins that elimintes customers who order less than 3 parts. It could be that that is the desired result, it is not obvious. 

I believe we should not filter out those customers.

### Is it easy to understand?
Yes, it could be improved by:
* Including brief and meaningful comments
* Not using initials to alias the tables
* Using column names in the group by clause

### Could the code be more efficient?
Yes, these are some things that could make it more efficient:  

* Remove order by in the CTEs
* There is no need to join to the part table